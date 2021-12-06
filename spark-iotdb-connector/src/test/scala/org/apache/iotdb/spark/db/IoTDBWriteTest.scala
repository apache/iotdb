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

import org.apache.iotdb.db.conf.IoTDBConstant
import org.apache.iotdb.db.service.IoTDB
import org.apache.iotdb.jdbc.Config
import org.apache.iotdb.session.Session
import org.apache.spark.sql.SparkSession
import org.junit.{AfterClass, Before}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class IoTDBWriteTest extends FunSuite with BeforeAndAfterAll {
  private var daemon: IoTDB = _
  private var spark: SparkSession = _
  private var session: Session = _

  @Before
  override protected def beforeAll(): Unit = {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/")
    super.beforeAll()

    EnvironmentUtils.closeStatMonitor()
    daemon = IoTDB.getInstance
    daemon.active()
    EnvironmentUtils.envSetUp()
    Class.forName(Config.JDBC_DRIVER_NAME)

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()

    session = new Session("127.0.0.1", 6667, "root", "root")
    session.open()
  }

  @AfterClass
  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

    daemon.stop()
    EnvironmentUtils.cleanEnv()

    session.close()
    super.afterAll()
  }

  test("test insert wide data") {
    val df = spark.createDataFrame(List(
      (1L, 1, 1L, 1.0F, 1.0D, true, "hello"),
      (2L, 2, 2L, 2.0F, 2.0D, false, "world")))

    val dfWithColumn = df.withColumnRenamed("_1", "Time")
      .withColumnRenamed("_2", "root.test.d0.int")
      .withColumnRenamed("_3", "root.test.d0.long")
      .withColumnRenamed("_4", "root.test.d0.float")
      .withColumnRenamed("_5", "root.test.d0.double")
      .withColumnRenamed("_6", "root.test.d0.boolean")
      .withColumnRenamed("_7", "root.test.d0.text")
    dfWithColumn.write.format("org.apache.iotdb.spark.db")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .save

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      size += 1
    }
    assertResult(2)(size)
  }

  test("test insert narrow data") {
    val df = spark.createDataFrame(List(
      (1L, "root.test.d0",1, 1L, 1.0F, 1.0D, true, "hello"),
      (2L, "root.test.d0", 2, 2L, 2.0F, 2.0D, false, "world")))

    val dfWithColumn = df.withColumnRenamed("_1", "Time")
      .withColumnRenamed("_2", "device_name")
      .withColumnRenamed("_3", "int")
      .withColumnRenamed("_4", "long")
      .withColumnRenamed("_5", "float")
      .withColumnRenamed("_6", "double")
      .withColumnRenamed("_7", "boolean")
      .withColumnRenamed("_8", "text")
    dfWithColumn.write.format("org.apache.iotdb.spark.db")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .save

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      size += 1
    }
    assertResult(2)(size)
  }
}
