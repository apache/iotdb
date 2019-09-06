package org.apache.iotdb.tsfile

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class IoTDBTest extends JUnitSuite {
  private val testFile = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val csvPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.csv"
  private val tsfilePath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val errorPath: String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/errortest.tsfile"
  private var sqlContext: SQLContext = _
  private var spark: SparkSession = _

  @Before
  def before(): Unit = {
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  @After
  def after(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

  }

  @Test
  def showData(): Unit = {
    val df = spark.read.format("org.apache.iotdb.tsfile").option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root").load
    df.printSchema()
    df.show()
  }

  @Test
  def showDataWithPartition(): Unit = {
    val df = spark.read.format("org.apache.iotdb.tsfile").option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root").option("lowerBound", 1).option("upperBound", System.nanoTime() / 1000 / 1000).option("numPartition", 10).load

    df.printSchema()

    df.show()
  }
}
