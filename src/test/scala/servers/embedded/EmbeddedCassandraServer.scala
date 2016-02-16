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
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.slf4j.LoggerFactory

trait EmbeddedCassandraServer extends ServerUtils {

  override val logger = LoggerFactory.getLogger(classOf[EmbeddedCassandraServer])

  val START_TIMEOUT = 15000
  val CASSANDRA_YAML = "cassandra.yaml"

  def startCassandra() {
    logger.info("starting embedded Cassandra server")
    cleanCassandraDir
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_YAML, START_TIMEOUT)
  }

  def stopCassandra() {
    logger.info("stopping embedded Cassandra server")
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    cleanCassandraDir
  }

  def cleanCassandraDir = {
    FileUtils.deleteQuietly(new File("target/embeddedCassandra"))
    FileUtils.deleteQuietly(new File("target/cassandra"))
  }
}
