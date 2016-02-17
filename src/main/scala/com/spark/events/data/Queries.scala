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

trait Queries {

  val KEYSPACE = "poctu"
  val CASSANDRA_NODES = "localhost"
  val PORT = 9042

  val INIT_DB_QUERIES = List(
    """
      |CREATE KEYSPACE IF NOT EXISTS """.stripMargin + KEYSPACE +
      """
      |WITH replication = {
      |	'class' : 'SimpleStrategy',
      |	'replication_factor' : 1
      |};
    """.stripMargin,

    """ CREATE TABLE IF NOT EXISTS """.stripMargin + KEYSPACE +
    """.status (
      |      id text,
      |      value text,
      |      PRIMARY KEY (id)
      |  );
    """.stripMargin
  )
}
