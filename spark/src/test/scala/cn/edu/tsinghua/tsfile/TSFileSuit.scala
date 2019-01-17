/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.iotdb.tsfile

import java.io.File


class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val resourcesFolder = "src/test/resources"
  private val tsfileFolder = "src/test/resources/tsfile"
  private val tsfile1 = "src/test/resources/tsfile/test1.tsfile"
  private val tsfile2 = "src/test/resources/tsfile/test2.tsfile"
  private val outputPath = "src/test/resources/output"
  private val outputPathFile = outputPath + "/part-m-00000"
  private val outputPath2 = "src/test/resources/output2"
  private val outputPathFile2 = outputPath2 + "/part-m-00000"
  private var spark: SparkSession = _

  def deleteDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.list().foreach(f => {
        deleteDir(new File(dir, f))
      })
    }
    dir.delete()

  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val resources = new File(resourcesFolder)
    if (!resources.exists())
      resources.mkdirs()
    val tsfile_folder = new File(tsfileFolder)
    if (!tsfile_folder.exists())
      tsfile_folder.mkdirs()
    val output = new File(outputPath)
    if (output.exists())
      deleteDir(output)
    val output2 = new File(outputPath2)
    if (output2.exists())
      deleteDir(output2)
    new CreateTSFile().createTSFile1(tsfile1)
    new CreateTSFile().createTSFile2(tsfile2)
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
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


  test("writer") {
    val df = spark.read.tsfile(tsfile1)
    df.show()
    df.write.tsfile(outputPath)
    val newDf = spark.read.tsfile(outputPathFile)
    newDf.show()
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write options") {
    val df = spark.read.option("delta_object_name", "root.carId.deviceId").tsfile(tsfile1)
    df.write.option("delta_object_name", "root.carId.deviceId") tsfile (outputPath2)
    val newDf = spark.read.option("delta_object_name", "root.carId.deviceId").tsfile(outputPathFile2)
    newDf.show()
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test read options") {
    val options = new mutable.HashMap[String, String]()
    options.put(SQLConstant.DELTA_OBJECT_NAME, "root.carId.deviceId")
    val df = spark.read.options(options).tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")

    spark.sql("select * from tsfile_table where deviceId = 'd1' and carId = 'car' and time < 10").show()
    val newDf = spark.sql("select * from tsfile_table where deviceId = 'd1'")
    Assert.assertEquals(4, newDf.count())
  }

  test("tsfile_qp") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select s1,s2 from tsfile_table where delta_object = 'root.car.d1' and time <= 10 and (time > 5 or s1 > 10)")
    Assert.assertEquals(0, newDf.count())
  }

  test("testMultiFilesNoneExistDelta_object") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where delta_object = 'd4'")
    Assert.assertEquals(0, newDf.count())
  }

  test("testMultiFilesWithFilterOr") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where s1 < 2 or s2 > 60")
    Assert.assertEquals(4, newDf.count())
  }

  test("testMultiFilesWithFilterAnd") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where s2 > 20 and s1 < 5")
    Assert.assertEquals(2, newDf.count())
  }

  test("testMultiFilesSelect*") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    Assert.assertEquals(16, newDf.count())
  }

  test("testCount") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select count(*) from tsfile_table")
    Assert.assertEquals(8, newDf.head().apply(0).asInstanceOf[Long])
  }

  test("testSelect *") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(8, count)
  }

  test("testQueryData1") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")

    val newDf = spark.sql("select s1, s3 from tsfile_table where s1 > 4 and delta_object = 'root.car.d2'").cache()
    val count = newDf.count()
    Assert.assertEquals(4, count)
  }

  test("testQueryDataComplex2") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")

    val newDf = spark.sql("select * from tsfile_table where s1 <4 and delta_object = 'root.car.d1' or s1 > 5 and delta_object = 'root.car.d2'").cache()
    val count = newDf.count()
    Assert.assertEquals(6, count)
  }

  test("testQuerySchema") {
    val df = spark.read.format("org.apache.iotdb.tsfile").load(tsfile1)

    val expected = StructType(Seq(
      StructField(SQLConstant.RESERVED_TIME, LongType, nullable = true),
      StructField(SQLConstant.RESERVED_DELTA_OBJECT, StringType, nullable = true),
      StructField("s3", FloatType, nullable = true),
      StructField("s4", DoubleType, nullable = true),
      StructField("s5", StringType, nullable = true),
      StructField("s1", IntegerType, nullable = true),
      StructField("s2", LongType, nullable = true)
    ))
    Assert.assertEquals(expected, df.schema)
  }

}