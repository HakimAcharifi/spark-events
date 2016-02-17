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

import com.spark.events.data.DataAccessManager
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import servers.embedded.EmbeddedServers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

trait TestBase extends FlatSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with EmbeddedServers
with DataAccessManager
with KafkaEventProducer {

  override def beforeAll(): Unit = {
    startEmbeddedServers()
    initDb()
  }

  override def afterAll(): Unit = {
    stopEmbeddedServers()
  }

  def generateEvents(nbEvents: Int, topic: String): Unit = {
    var listEvents = List.empty[Event]
    val dBUpdateEvent = DBUpdateEvent("DB_" + System.currentTimeMillis().toString, generateRandomStatusValue)
    val txEventValue = dBUpdateEvent.id + ":" + dBUpdateEvent.value
    for (id <- 1 to nbEvents - 1) {
      listEvents = TxEvent("TX_" + id.toString, txEventValue) :: listEvents
    }
    listEvents = dBUpdateEvent :: listEvents

    // wait until all events get sent
    Await.ready(Future.sequence(sendToKafka(topic, listEvents)), 10 seconds)
  }

  def createTopic(topic: String, nbPartitions: Int): Unit = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val zkClient = new ZkClient("localhost:2181", sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, nbPartitions, 1, new Properties)
  }

  def generateRandomStatusValue: String = (Random.alphanumeric take 10 mkString).toUpperCase

}
