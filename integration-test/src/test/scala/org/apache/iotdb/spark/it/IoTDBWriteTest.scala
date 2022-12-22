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

package org.apache.iotdb.spark.it

import org.apache.iotdb.it.env.EnvFactory
import org.apache.iotdb.it.framework.IoTDBTestRunner
import org.apache.iotdb.itbase.category.{ClusterIT, LocalStandaloneIT}
import org.apache.iotdb.session.ISession
import org.apache.spark.sql.SparkSession
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.{AfterClass, Before}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[IoTDBTestRunner])
@Category(Array(classOf[LocalStandaloneIT], classOf[ClusterIT]))
class IoTDBWriteTest extends FunSuite with BeforeAndAfterAll {
//  private var daemon: NewIoTDB = _
  private var spark: SparkSession = _
  private var session: ISession = _
  private var jdbcUrlTemplate = "jdbc:iotdb://%s:%s/"

  @Before
  override protected def beforeAll(): Unit = {
    System.setProperty("IOTDB_CONF", "src/test/resources/")
    super.beforeAll()

//    daemon = NewIoTDB.getInstance
//    daemon.active(false)
    EnvFactory.getEnv.initBeforeClass()
    jdbcUrlTemplate = jdbcUrlTemplate.format(EnvFactory.getEnv.getIP, EnvFactory.getEnv.getPort)

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()

    session = EnvFactory.getEnv.getSessionConnection
    session.open()
  }

  @AfterClass
  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

//    daemon.stop()
    session.close()
    EnvFactory.getEnv.cleanAfterTest()
    super.afterAll()
  }

  test("test insert wide data") {
    val df = spark.createDataFrame(List(
      (1L, 1, 1L, 1.0F, 1.0D, true, "hello"),
      (2L, 2, 2L, 2.0F, 2.0D, false, "world")))

    val dfWithColumn = df.withColumnRenamed("_1", "Time")
      .withColumnRenamed("_2", "root.test.d0.s0")
      .withColumnRenamed("_3", "root.test.d0.s1")
      .withColumnRenamed("_4", "root.test.d0.s2")
      .withColumnRenamed("_5", "root.test.d0.s3")
      .withColumnRenamed("_6", "root.test.d0.s4")
      .withColumnRenamed("_7", "root.test.d0.s5")
    dfWithColumn.write.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .save

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      result.next()
      size += 1
    }
    assertResult(2)(size)
  }

  test("test insert narrow data") {
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
    dfWithColumn.write.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .save

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      result.next()
      size += 1
    }
    assertResult(2)(size)
  }
}
