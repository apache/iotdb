package org.apache.iotdb.tsfile

import java.io.File

import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.tool.TsFileWrite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val resourcesFolder = "../spark/src/test/resources"
  private val tsfileFolder = "../spark/src/test/resources/tsfile"
  private val tsfile1 = "../spark/src/test/resources/tsfile/test1.tsfile"
  private val tsfile2 = "../spark/src/test/resources/tsfile/test2.tsfile"
  private val outputPath = "../spark/src/test/resources/output"
  private val outputPathFile = outputPath + "/part-m-00000"
  private val outputPath2 = "src/test/resources/output2"
  private val outputPathFile2 = outputPath2 + "/part-m-00000"
  private val outputHDFSPath = "hdfs://localhost:9000/usr/hadoop/output"
  private val outputHDFSPathFile = outputHDFSPath + "/part-m-00000"
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\winutils")
    System.setProperty("hadoop.home.dir", "/home/rl/usr/local/hadoop")
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
    new TsFileWrite().create1(tsfile1)
    new TsFileWrite().create2(tsfile2)
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
    df.show()
    df.write.tsfile(outputPath)
    val newDf = spark.read.tsfile(outputPathFile)
    newDf.show()
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write 2") {
    val df = spark.read.tsfile(tsfile2)
    df.write.tsfile(outputPath2)
    val newDf = spark.read.tsfile(outputPathFile2)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write to HDFS") {
    val df = spark.read.tsfile(tsfile2)
    df.write.tsfile(outputHDFSPath)
    val newDf = spark.read.tsfile(outputHDFSPathFile)
    val count = newDf.count()
    Assert.assertEquals(TsFileWrite.largeNum, count)
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
    Assert.assertEquals(TsFileWrite.largeNum, count)
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

  test("testMultiFiles") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    df.show()
    Assert.assertEquals(13632517, df.count())
  }

  test("testMultiFilesWithFilter1") {
    val df = spark.read.tsfile(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1` >0 " +
      "and `device_1.sensor_1` <10 or `device_1.sensor_2` >0")
    //    newDf.show()
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

}