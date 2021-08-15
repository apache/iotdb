/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.spark.db

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

private[iotdb] class DefaultSource extends RelationProvider with DataSourceRegister {
  private final val logger = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = "tsfile"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    val iotdbOptions = new IoTDBOptions(parameters)

    if (iotdbOptions.url == null || iotdbOptions.sql == null) {
      sys.error("IoTDB url or sql not specified")
    }
    new IoTDBRelation(iotdbOptions)(sqlContext.sparkSession)

  }
}