/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spark.events.processing

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object NoMapWaitEventProcessing extends Serializable with EventProcessing {

  override val log = LoggerFactory.getLogger("com.spark.events.processing.OneStageEventProcessing")

  def startProcess(paramsMap: Map[String, String]): Unit = {

    val batchInterval = paramsMap.getOrElse(BATCH_INTERVAL, "1").toInt
    val nbCores = paramsMap.getOrElse(NB_CORES, "2").toInt
    val sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[" + nbCores + "]")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))
    val outputId = paramsMap.get(OUTPUT_RESULT_ID).get

    doProcess(paramsMap, sparkConf, ssc).map(
      event => {
        // Ici, nous nous trouvons toujours dans le meme stage
        // donc il est probable que certains TXEvent de "voient"
        // pas les effets de l'event DBUpdateEvent de la map
        // precedente dans cassandra, ce qui correspond au resultat
        // attendu. Mais pour ce faire, il est necessaire que le topic
        // source soit suffisamment partionne (#partitions > 1)
        val expectedStatus = event.value.split(":")(1)
        readFromCassandra(event.value.split(":")(0)) match {
          case Some(status) if status.equals(expectedStatus) => true
          case _ => false
        }
      }
      // si l'un au moins des TXEvent ne "voit" pas les effets de la map precedente,
      // alors le resultat final est "false" et le test est OK
    ).reduce((b1, b2) => b1 && b2).foreachRDD(
      rdd => rdd.foreach(boolean => writeToCassandra(outputId, boolean.toString))
    )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
