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

import java.io.ByteArrayOutputStream
import org.apache.iotdb.it.env.EnvFactory
import org.apache.iotdb.it.framework.IoTDBTestRunner
import org.apache.iotdb.itbase.category.{ClusterIT, LocalStandaloneIT}
import org.apache.iotdb.spark.db.Transformer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.SQLException
import java.util.Locale

@RunWith(classOf[IoTDBTestRunner])
@Category(Array(classOf[LocalStandaloneIT], classOf[ClusterIT]))
class IoTDBTest extends FunSuite with BeforeAndAfterAll {
//  private var daemon: NewIoTDB = _

  private val creationSqls = Array[String]("CREATE DATABASE root.vehicle.d0", "CREATE DATABASE root.vehicle.d1", "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE", "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE", "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE", "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN", "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN")
  private val dataSet2 = Array[String]("CREATE DATABASE root.ln.wf01.wt01", "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN", "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN", "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN", "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) " + "values(1, 1.1, false, 11)", "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) " + "values(2, 2.2, true, 22)", "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) " + "values(3, 3.3, false, 33 )", "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) " + "values(4, 4.4, false, 44)", "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) " + "values(5, 5.5, false, 55)")
  private val insertTemplate = "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)"
  private var jdbcUrlTemplate = "jdbc:iotdb://%s:%s/"

  private val testFile = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val csvPath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.csv"
  private val tsfilePath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val errorPath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/errortest.tsfile"
  private var sqlContext: SQLContext = _
  private var spark: SparkSession = _

  @Before
  override protected def beforeAll(): Unit = {
    System.setProperty("IOTDB_CONF", "src/test/resources/")
    super.beforeAll()

//    daemon = NewIoTDB.getInstance
//    daemon.active(false)
    EnvFactory.getEnv.initBeforeClass()
    jdbcUrlTemplate = jdbcUrlTemplate.format(EnvFactory.getEnv.getIP, EnvFactory.getEnv.getPort)
    prepareData()

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  @AfterClass
  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

//    daemon.stop()
    EnvFactory.getEnv.cleanAfterTest()

    super.afterAll()
  }

  @throws[SQLException]
  def prepareData(): Unit = {
    try {
      val connection = EnvFactory.getEnv.getConnection()
      val statement = connection.createStatement
      try {
        for (sql <- creationSqls) {
          statement.execute(sql)
        }
        for (sql <- dataSet2) {
          statement.execute(sql)
        }
        // prepare BufferWrite file
        for (i <- 5000 until 7000) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "true"))
        }
        statement.execute("flush")
        for (i <- 7500 until 8500) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "false"))
        }
        statement.execute("flush")
        // prepare Unseq-File
        for (i <- 500 until 1500) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "true"))
        }
        statement.execute("flush")
        for (i <- 3000 until 6500) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "false"))
        }
        statement.execute("merge")
        // prepare BufferWrite cache
        for (i <- 9000 until 10000) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "true"))
        }
        // prepare Overflow cache
        for (i <- 2000 until 2500) {
          statement.execute(insertTemplate.formatLocal(Locale.ENGLISH, i, i, i, i.toDouble, "'" + i + "'", "false"))
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  test("test show data") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate).option("sql", "select ** from root").load
    Assert.assertEquals(7505, df.count())
  }

  test("test show data with partition") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root")
      .option("lowerBound", 1).option("upperBound", System.nanoTime() / 1000 / 1000)
      .option("numPartition", 10).load
    Assert.assertEquals(7505, df.count())
  }

  test("test filter data") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root where time < 2000 and time > 1000").load

    Assert.assertEquals(499, df.count())
  }

  test("test filter data with partition") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root where time < 2000 and time > 1000")
      .option("lowerBound", 1)
      .option("upperBound", 10000).option("numPartition", 10).load

    Assert.assertEquals(499, df.count())
  }

  test("test transform to narrow") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root where time < 1100 and time > 1000").load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    Assert.assertEquals(198, narrow_df.count())
  }

  test("test transform to narrow with partition") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root where time < 1100 and time > 1000")
      .option("lowerBound", 1).option("upperBound", 10000)
      .option("numPartition", 10).load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    Assert.assertEquals(198, narrow_df.count())
  }

  test("test transform back to wide") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select ** from root where time < 1100 and time > 1000").load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    val wide_df = Transformer.toWideForm(spark, narrow_df)
    Assert.assertEquals(99, wide_df.count())
  }

  test("test aggregate sql") {
    val df = spark.read.format("org.apache.iotdb.spark.db")
      .option("url", jdbcUrlTemplate)
      .option("sql", "select count(d0.s0),count(d0.s1) from root.vehicle").load

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(df.count.toInt, false)
    }
    val actual = outCapture.toByteArray.map(_.toChar)
    val expect =
      "+-------------------------+-------------------------+\n" +
        "|count(root.vehicle.d0.s0)|count(root.vehicle.d0.s1)|\n" +
        "+-------------------------+-------------------------+\n" +
        "|7500                     |7500                     |\n" +
        "+-------------------------+-------------------------+\n"

    Assert.assertArrayEquals(expect.toCharArray, actual.dropRight(2))
  }
}

