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

import java.io.{File, FileWriter}

import com.spark.events.data.DataAccessManager
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

trait EventProcessing extends Serializable
with EventsTrait
with DataAccessManager {

  val BROKERS = "brokers"
  val TOPICS = "topics"
  val MAP_DURATION = "map-duration"
  val BATCH_INTERVAL = "batch-interval-seconds"
  val NB_CORES = "nb-cores"
  val OUTPUT_RESULT_ID = "output-id"
  val APP_NAME = "EVENT_PROCESSING_APP"

  def doProcess(paramsMap: Map[String, String], sparkConf: SparkConf, ssc: StreamingContext): DStream[Event] = {
    val brokers = paramsMap.getOrElse(BROKERS, "localhost:9092")
    val topics = paramsMap.getOrElse(TOPICS, "EVENTS")
    val mapDuration = paramsMap.getOrElse(MAP_DURATION, "5000").toInt

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val eventsStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    eventsStream.map {

      case event if event._1.startsWith("TX") => TxEvent(event._1, event._2)

      case event if event._1.startsWith("DB") => DBUpdateEvent(event._1, event._2)

    }.map[Event] {

      case txEvent: TxEvent => txEvent

      case updateEvent: DBUpdateEvent => {
        // first wait
        try {
          Await.ready(Future{while(true){}}, mapDuration milliseconds)
        }catch {
          case _: Throwable => // do nothing
        }
        // then apply db update
        writeToCassandra(updateEvent.id, updateEvent.value)
        // finally, return the event
        updateEvent
      }
    }.filter(event => event.isInstanceOf[TxEvent])
  }

  def print(text: String, outputFile: String): Unit = {
    val writer = new FileWriter(new File(outputFile), true)
    writer.write(text + "\n")
    writer.flush()
    writer.close()
  }
}
