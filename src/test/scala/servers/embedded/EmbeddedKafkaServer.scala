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

import java.io.File

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

trait EmbeddedKafkaServer extends ServerUtils{

  override val logger = LoggerFactory.getLogger(classOf[EmbeddedKafkaServer])

  val kafkaConfig = loadProperties("/kafka.bootstrap.properties")
  val kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaConfig))
  val KAFKA_DIR = "target/kafka"

  def startKafka() {
    logger.info("starting embedded Kafka server")
    FileUtils.deleteQuietly(new File(KAFKA_DIR))
    kafkaServer.startup()
  }

  def stopKafka() {
    logger.info("stopping embedded Kafka server")
    kafkaServer.shutdown()
    FileUtils.deleteQuietly(new File(KAFKA_DIR))
  }
}


