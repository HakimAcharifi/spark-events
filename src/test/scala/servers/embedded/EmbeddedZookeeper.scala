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

import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.slf4j.LoggerFactory

trait EmbeddedZookeeper extends ServerUtils{

  override val logger = LoggerFactory.getLogger(classOf[EmbeddedZookeeper])

  val zkServer = new ZooKeeperServerMain
  val quorumConfiguration = new QuorumPeerConfig
  val zkConfig = new ServerConfig
  val zkProps = loadProperties("/zookeeper.properties")
  val ZK_DIR = "target/zookeeper"

  def cleanZookeeper = FileUtils.deleteQuietly(new File(ZK_DIR))

  def startZookeeper() {
    logger.info("starting embedded Zookeeper server")
    cleanZookeeper
    quorumConfiguration.parseProperties(zkProps)
    zkConfig.readFrom(quorumConfiguration)
    new Thread(
      new Runnable {
        override def run() = zkServer.runFromConfig(zkConfig)
      }
    ).start()
  }

  def stopZookeeper() {
    logger.info("stopping embedded Zookeeper server")
    cleanZookeeper
    // TODO stop zookeeper (is it necessary ?)
  }
}
