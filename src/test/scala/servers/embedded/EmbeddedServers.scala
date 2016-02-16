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
package servers.embedded

import org.slf4j.LoggerFactory

trait EmbeddedServers extends ServerUtils
with EmbeddedCassandraServer
with EmbeddedKafkaServer
with EmbeddedZookeeper {

  override val logger = LoggerFactory.getLogger(classOf[EmbeddedServers])

  def startEmbeddedServers() {
    logger.info("starting all embedded servers")
    startCassandra()
    startKafkaWithZookeeper()
  }

  def stopEmbeddedServers() {
    logger.info("starting all embedded servers")
    stopCassandra()
    stopKafkaWithZookeeper()
  }

  def startKafkaWithZookeeper() {
    logger.info("starting Kafka and Zookeeper")
    startZookeeper()
    startKafka()
  }

  def stopKafkaWithZookeeper() {
    logger.info("stopping Kafka and Zookeeper")
    stopKafka()
    stopZookeeper()
  }
}
