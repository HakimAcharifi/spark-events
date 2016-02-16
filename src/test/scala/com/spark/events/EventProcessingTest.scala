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
package com.spark.events

import com.spark.events.processing.{MapWaitEventProcessing, NoMapWaitEventProcessing}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.Breaks

class EventProcessingTest extends TestBase {

  val MyApp = "SparkEventProcessing"

  override val log = LoggerFactory.getLogger(classOf[EventProcessingTest])

  MyApp should "wait map operation termination before running next stage transformations" in {

    val topic = "EVENTS_" + System.currentTimeMillis()
    val outputId = System.currentTimeMillis().toString
    val paramsMap = Map[String, String](
      "brokers" -> "localhost:9092",
      "topics" -> topic,
      "nb-cores" -> "5",
      "map-duration" -> "5000",
      "batch-interval-seconds" -> "2",
      "output-id" -> outputId
    )

    // create topic with 4 partitions
    createTopic(topic, 4)
    // first send some event to kafka
    generateEvents(100, topic)
    // start event processing
    Future(MapWaitEventProcessing.startProcess(paramsMap))

    // wait until first batch interval stream processing result get written to cassandra
    Breaks.breakable(
      while (true) {
        Thread.sleep(paramsMap.get("map-duration").get.toInt)
        if (readFromCassandra(outputId).isDefined) Breaks.break()
      }
    )

    // now check the result from cassandra
    val result = readFromCassandra(outputId) match {
      case Some(storedValue) if storedValue.toBoolean => true
      case _ => false
    }
    result shouldBe true
  }

  MyApp should "execute next stage transformations without waiting map operation termination" in {

    val topic = "EVENTS_" + System.currentTimeMillis()
    val outputId = System.currentTimeMillis().toString
    val paramsMap = Map[String, String](
      "brokers" -> "localhost:9092",
      "topics" -> topic,
      "nb-cores" -> "6",
      "map-duration" -> "5000",
      "batch-interval-seconds" -> "2",
      "output-id" -> outputId
    )

    // create topic with 4 partitions
    createTopic(topic, 4)
    // first send some event to kafka
    generateEvents(100, topic)
    // start event processing
    Future(NoMapWaitEventProcessing.startProcess(paramsMap))

    // wait until first batch interval stream processing result get written to cassandra
    Breaks.breakable(
      while (true) {
        Thread.sleep(paramsMap.get("map-duration").get.toInt)
        if (readFromCassandra(outputId).isDefined) Breaks.break()
      }
    )

    // now check the result
    val result = readFromCassandra(outputId) match {
      case Some(storedValue) if storedValue.toBoolean => true
      case _ => false
    }
    result shouldBe false
  }
}
