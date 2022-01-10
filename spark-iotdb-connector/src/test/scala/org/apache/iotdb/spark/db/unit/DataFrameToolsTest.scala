package org.apache.iotdb.spark.db.unit

import org.apache.iotdb.db.conf.IoTDBConstant
import org.apache.iotdb.db.service.IoTDB
import org.apache.iotdb.jdbc.Config
import org.apache.iotdb.session.Session
import org.apache.iotdb.spark.db.{DataFrameTools, EnvironmentUtils, IoTDBOptions}
import org.apache.spark.sql.SparkSession
import org.junit._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DataFrameToolsTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var daemon: IoTDB = _
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
    EnvironmentUtils.prepareData()

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

    daemon.stop()
    EnvironmentUtils.cleanEnv()

    session.close()
    super.afterAll()
  }

  test("test insertDataFrame method") {
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

    val optionsMap = Map("url" -> "jdbc:iotdb://127.0.0.1:6667/", "numPartition" -> "1")
    val options = new IoTDBOptions(optionsMap)

    DataFrameTools.insertDataFrame(options ,dfWithColumn)

    val result = session.executeQueryStatement("select ** from root")
    var size = 0
    while (result.hasNext) {
      size += 1
    }
    assertResult(2)(size)
  }
}
