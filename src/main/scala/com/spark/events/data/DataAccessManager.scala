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
package com.spark.events.data

import com.datastax.driver.core._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

trait DataAccessManager extends Queries {

  val log = LoggerFactory.getLogger(classOf[DataAccessManager])

  def initDb() = {
    log.info("initializing database")
    executeWithSession { session =>
      INIT_DB_QUERIES.foreach(query => session.execute(query))
    }
  }

  def readFromCassandra(id: String): Option[String] = {
    executeWithSession(session => {

      Try(session.execute("select * from " + KEYSPACE + ".status where id='" + id + "';")) match {

        case Failure(ex) => throw ex

        case Success(result: ResultSet) => {

          Option(result.one()) match {
            case Some(row) => Option(row.getString("value"))
            case _ => None
          }
        }
      }
    })
  }

  def writeToCassandra(id: String, value: String): Unit = {
    executeWithSession(session => {
      Try(session.execute("insert into " + KEYSPACE + ".status(id, value) values('" + id + "', '" + value + "');")) match {
        case Failure(ex) => throw ex
        case _ => // do nothing
      }
    })
  }

  def executeWithSession[T](query: Session => T): T = {
    val cluster = Cluster.builder().addContactPoints(CASSANDRA_NODES.split(",").map(_.trim).toList: _*)
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
      .withPort(PORT).build()
    val session: Session = cluster.connect
    val result = query(session)
    //closeQuietly(session)
    //closeQuietly(cluster)
    result
  }

  def closeQuietly(session: Session): Unit = {
    try {
      session.close()
    }catch {
      case ex: Throwable => // do nothing
    }
  }

  def closeQuietly(cluster: Cluster): Unit = {
    try {
      cluster.close()
    }catch {
      case ex: Throwable => // do nothing
    }
  }
}