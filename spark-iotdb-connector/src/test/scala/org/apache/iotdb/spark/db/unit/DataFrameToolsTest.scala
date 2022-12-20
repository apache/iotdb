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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.spark.db.unit

import org.apache.iotdb.jdbc.Config
import org.apache.iotdb.session.Session
import org.apache.iotdb.spark.db.{DataFrameTools, EnvironmentUtils, IoTDBOptions}
import org.apache.spark.sql.SparkSession
import org.junit._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

// TODO move it to integration-test
@Ignore
class DataFrameToolsTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
//  private var daemon: NewIoTDB = _
  private var session: Session = _

  @Before
  override protected def beforeAll(): Unit = {
    System.setProperty("IOTDB_CONF", "src/test/resources/")
    super.beforeAll()

//    daemon = NewIoTDB.getInstance
//    daemon.active(false)
    EnvironmentUtils.envSetUp()
    Class.forName(Config.JDBC_DRIVER_NAME)

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("unit test")
      .getOrCreate()

    session = new Session("127.0.0.1", 6667, "root", "root")
    session.open()
  }

  @AfterClass
  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

//    daemon.stop()
    EnvironmentUtils.cleanEnv()

    session.close()
    super.afterAll()
  }

  test("test insertDataFrame method") {
    val df = spark.createDataFrame(List(
      (1L, "root.test.d0",1, 1L, 1.0F, 1.0D, true, "hello"),
      (2L, "root.test.d0", 2, 2L, 2.0F, 2.0D, false, "world")))

    val dfWithColumn = df.withColumnRenamed("_1", "Time")
      .withColumnRenamed("_2", "Device")
      .withColumnRenamed("_3", "s0")
      .withColumnRenamed("_4", "s1")
      .withColumnRenamed("_5", "s2")
      .withColumnRenamed("_6", "s3")
      .withColumnRenamed("_7", "s4")
      .withColumnRenamed("_8", "s5")

    val optionsMap = Map("url" -> "jdbc:iotdb://127.0.0.1:6667/", "numPartition" -> "1")
    val options = new IoTDBOptions(optionsMap)

    DataFrameTools.insertDataFrame(options ,dfWithColumn)

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      result.next()
      size += 1
    }
    assertResult(2)(size)
  }
}