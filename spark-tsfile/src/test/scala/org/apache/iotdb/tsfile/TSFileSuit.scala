/**
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
package org.apache.iotdb.tsfile

import java.io.File

import org.apache.iotdb.tool.TsFileWriteTool
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val resourcesFolder = "../spark/src/test/resources"
  private val tsfileFolder1 = resourcesFolder + "/tsfile1"
  private val tsfileFolder2 = resourcesFolder + "/tsfile2"
  private val tsfile1 = tsfileFolder1 + "/test1.tsfile"
  private val tsfile2 = tsfileFolder1 + "/test2.tsfile"
  private val tsfile3 = tsfileFolder2 + "/test.tsfile"
  private val outputPath = "../spark/src/test/resources/output"
  private val outputPathFile = outputPath + "/part-m-00000"
  private val outputPath2 = "../spark/src/test/resources/output2"
  private val outputPathFile2 = outputPath2 + "/part-m-00000"
  private val outputPath3 = "../spark/src/test/resources/output3"
  private val outputPathFile3 = outputPath3
  private val outputHDFSPath = "hdfs://localhost:9000/usr/hadoop/output"
  private val outputHDFSPathFile = outputHDFSPath + "/part-m-00000"
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val resources = new File(resourcesFolder)
    if (!resources.exists())
      resources.mkdirs()

    val tsfile_folder1 = new File(tsfileFolder1)
    if (tsfile_folder1.exists()) {
      deleteDir(tsfile_folder1)
    }
    tsfile_folder1.mkdirs()

    val tsfile_folder2 = new File(tsfileFolder2)
    if (tsfile_folder2.exists()) {
      deleteDir(tsfile_folder2)
    }
    tsfile_folder2.mkdirs()

    new TsFileWriteTool().create1(tsfile1)
    new TsFileWriteTool().create2(tsfile2)
    new TsFileWriteTool().create3(tsfile3)

    val output = new File(outputPath)
    if (output.exists())
      deleteDir(output)
    val output2 = new File(outputPath2)
    if (output2.exists())
      deleteDir(output2)

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    val folder1 = new File(tsfileFolder1)
    deleteDir(folder1)
    val folder2 = new File(tsfileFolder2)
    deleteDir(folder2)
    val out = new File(outputPath)
    deleteDir(out)
    val out2 = new File(outputPath2)
    deleteDir(out2)
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  def deleteDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.list().foreach(f => {
        deleteDir(new File(dir, f))
      })
    }
    dir.delete()
  }

  test("test write 1") {
    val df = spark.read.tsfile(tsfile1)
    df.write.tsfile(outputPath)
    val newDf = spark.read.tsfile(outputPathFile)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write 2") {
    val df = spark.read.tsfile(tsfile2)
    df.write.tsfile(outputPath2)
    val newDf = spark.read.tsfile(outputPathFile2)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write 3") {
    val df = spark.read.tsfile(tsfile3, isNarrowForm = true)
    df.write.tsfile(outputPath3, isNarrowForm = true)
    val newDf = spark.read.tsfile(outputPathFile3, isNarrowForm = true)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("testSelect * from tsfile1") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(7, count)
  }

  test("testSelect * from tsfile2") {
    val df = spark.read.tsfile(tsfile2)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(TsFileWriteTool.largeNum, count)
  }

  test("testCount") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select count(*) from tsfile_table")
    Assert.assertEquals(7, newDf.head().apply(0).asInstanceOf[Long])
  }

  test("testSelect time") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time from tsfile_table")
    Assert.assertEquals(7, newDf.count())
  }

  test("testSelectString") {
    val df = spark.read.tsfile(tsfile3)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_2` = \"Monday\"")
    Assert.assertEquals(1, newDf.count())
  }

  test("testSelectBoolean") {
    val df = spark.read.tsfile(tsfile3)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1` = true")
    Assert.assertEquals(2, newDf.count())
  }

  test("testSelectWithFilterAnd") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1`>0 and `device_1.sensor_2` < 22")
    Assert.assertEquals(5, newDf.count())
  }

  test("testSelectWithFilterOr") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1`>0 or `device_1.sensor_2` < 22")
    Assert.assertEquals(7, newDf.count())
  }

  test("testSelectComplex") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where (`device_1.sensor_1`>0 or `device_1.sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testMultiFiles") {
    val df = spark.read.tsfile(tsfileFolder1)
    df.createOrReplaceTempView("tsfile_table")
    Assert.assertEquals(TsFileWriteTool.largeNum + 7, df.count())
  }

  test("testMultiFilesWithFilter1") {
    val df = spark.read.tsfile(tsfileFolder1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1` >0 " +
      "and `device_1.sensor_1` <10 or `device_1.sensor_2` >0")
    Assert.assertEquals(16, newDf.count())
  }

  test("testQuerySchema") {
    val df = spark.read.format("org.apache.iotdb.tsfile").load(tsfile1)

    val expected = StructType(Seq(
      StructField(QueryConstant.RESERVED_TIME, LongType, nullable = true),
      StructField("device_1.sensor_3", IntegerType, nullable = true),
      StructField("device_1.sensor_1", FloatType, nullable = true),
      StructField("device_1.sensor_2", IntegerType, nullable = true),
      StructField("device_2.sensor_3", IntegerType, nullable = true),
      StructField("device_2.sensor_1", FloatType, nullable = true),
      StructField("device_2.sensor_2", IntegerType, nullable = true)
    ))
    Assert.assertEquals(expected, df.schema)
  }

  test("testTransform1") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where (`device_1.sensor_1`>0 or `device_1.sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testTransform2") {
    val df = spark.read.tsfile(tsfile1, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where `device_name` = 'device_1' and (`sensor_1`>0 or `sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testTransform3") {
    val df = spark.read.tsfile(tsfile1, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where (`sensor_1`>0 or `sensor_2` < 22) and time < 4")
    Assert.assertEquals(5, newDf.count())
  }

}