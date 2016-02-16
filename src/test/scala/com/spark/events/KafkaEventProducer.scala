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

import java.util.Properties

import com.spark.events.processing.EventsTrait
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

object KafkaEventProducer {
  val kafkaProducerConfig = new Properties()
  kafkaProducerConfig.setProperty("zookeeper.connect", "localhost:2181")
  kafkaProducerConfig.setProperty("bootstrap.servers", "localhost:9092")
  kafkaProducerConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerConfig.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val logger = LoggerFactory.getLogger(classOf[KafkaEventProducer])
  val producer = new KafkaProducer[String, String](kafkaProducerConfig)
}

trait KafkaEventProducer extends EventsTrait {

  import KafkaEventProducer._

  def sendToKafka(topic: String, event: Event): Future[RecordMetadata] = {

    val promise = Promise[RecordMetadata]

    Future(producer.send(new ProducerRecord[String, String](topic, event.id, event.value),
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
          Option(exception) match {
            case Some(ex) => {
              promise.failure(ex)
              logger.error("an error occurred while sending message to kafka: [{}]", ex.getMessage)
            }
            case _ => {
              promise.success(metadata)
              val logParams = List(event.id, event.value, metadata.topic, metadata.partition, metadata.offset)
              logger.debug(
                """
                  |Event {
                  | key : {},
                  | value : {},
                  | topic : {},
                  | partition : {},
                  | offset : {}
                  |}
                  |successfully sent to kafka
                """.stripMargin, logParams.toSeq.asInstanceOf[Seq[AnyRef]]: _*
              )
            }
          }
        }
      }))
    promise.future
  }

  def sendToKafka(topic: String, events: List[Event]): List[Future[RecordMetadata]] = {

    def recurSendToKafka(remainingMsgList: List[Event], accumulo: List[Future[RecordMetadata]]): List[Future[RecordMetadata]] = {
      remainingMsgList match {
        case Nil => accumulo
        case head :: xs => recurSendToKafka(xs, sendToKafka(topic, head) :: accumulo)
      }
    }
    recurSendToKafka(events, Nil)
  }
}
