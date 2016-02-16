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

object MapWaitEventProcessing extends Serializable with EventProcessing {

  override val log = LoggerFactory.getLogger("com.spark.events.processing.TwoStageEventProcessing")

  def startProcess(paramsMap: Map[String, String]): Unit = {

    val batchInterval = paramsMap.getOrElse(BATCH_INTERVAL, "1").toInt
    val nbCores = paramsMap.getOrElse(NB_CORES, "5").toInt
    val sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[" + nbCores + "]")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))
    val outputId = paramsMap.get(OUTPUT_RESULT_ID).get

    doProcess(paramsMap, sparkConf, ssc).map(event => event).transform(

      // creation d'une operation necessitant un shuffle
      // ce type de transformation marque la frontiere
      // entre le stage precedent et le stage suivant
      rdd => rdd.groupBy(_.id).flatMap(_._2)
    ).map(
        event => {
          // l'attribut value de chaque TXEvent contient
          // la valeur de l'attribut de l'unique event DBUpdateEvent.
          // Comme on se trouve là dans un nouveau stage, on
          // s'attend a ce que chacun de ces TXEvent "voit" les
          // effets de l'event DBUpdateEvent du stage precedent dans cassandra.
          val expectedStatus = event.value.split(":")(1)
          readFromCassandra(event.value.split(":")(0)) match {
            case Some(status) if status.equals(expectedStatus) => true
            case _ => false
          }
        }
    // si l'un au moins des TXEvent ne "voit" pas les effets du stage precedent,
    // alors le resultat final est "false" et le test echoue. Il faudrait donc
    // que tous les TXEvent "voient" l'update du stage precedent
    ).reduce((b1, b2) => b1 && b2).foreachRDD(
      rdd => rdd.foreach(boolean => writeToCassandra(outputId, boolean.toString))
    )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
