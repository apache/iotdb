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

package org.apache.iotdb.spark.tsfile

import java.io.File
import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.iotdb.hadoop.fileSystem.HDFSInput
import org.apache.iotdb.spark.constant.TestConstant
import org.apache.iotdb.spark.tool.TsFileWriteTool
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType
import org.apache.iotdb.tsfile.read.TsFileSequenceReader
import org.apache.iotdb.tsfile.read.common.Field
import org.apache.iotdb.tsfile.utils.Binary
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ConverterTest extends FunSuite with BeforeAndAfterAll {
  private val tsfileFolder = TestConstant.BASE_OUTPUT_PATH.concat("ConverterTest")
  private val tsfilePath1: String = tsfileFolder + "/test_1.tsfile"
  private val tsfilePath2: String = tsfileFolder + "/test_2.tsfile"
  private var spark: SparkSession = _
  private var conf: Configuration = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val tsfile_folder = new File(tsfileFolder)
    if (tsfile_folder.exists()) {
      deleteDir(tsfile_folder)
    }
    tsfile_folder.mkdirs()
    new TsFileWriteTool().create1(tsfilePath1)
    new TsFileWriteTool().create2(tsfilePath2)
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
    conf = spark.sparkContext.hadoopConfiguration
  }

  override protected def afterAll(): Unit = {
    val folder = new File(tsfileFolder)
    deleteDir(folder)
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

  test("getSeries") {
    val in = new HDFSInput(new Path(new URI(tsfilePath1)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata

    val series = WideConverter.getSeries(tsFileMetaData, reader)

    Assert.assertEquals(6, series.size())
    Assert.assertEquals("[device_1.sensor_3,INT32]", series.get(0).toString)
    Assert.assertEquals("[device_1.sensor_1,FLOAT]", series.get(1).toString)
    Assert.assertEquals("[device_1.sensor_2,INT32]", series.get(2).toString)
    Assert.assertEquals("[device_2.sensor_3,INT32]", series.get(3).toString)
    Assert.assertEquals("[device_2.sensor_1,FLOAT]", series.get(4).toString)
    Assert.assertEquals("[device_2.sensor_2,INT32]", series.get(5).toString)

    in.close()
  }

  test("getUnionSeries") {
    val path1: Path = new Path(new URI(tsfilePath1))
    val fs1: FileSystem = path1.getFileSystem(conf)
    val status1 = fs1.getFileStatus(path1)

    val path2: Path = new Path(new URI(tsfilePath2))
    val fs2: FileSystem = path2.getFileSystem(conf)
    val status2 = fs2.getFileStatus(path2)

    val statusSeq: Seq[FileStatus] = Seq(status1, status2)

    val tsfileSchema = WideConverter.getUnionSeries(statusSeq, conf)

    Assert.assertEquals(tsfileSchema.size(), 6)
    Assert.assertEquals("[device_1.sensor_3,INT32]", tsfileSchema.get(0).toString)
    Assert.assertEquals("[device_1.sensor_1,FLOAT]", tsfileSchema.get(1).toString)
    Assert.assertEquals("[device_1.sensor_2,INT32]", tsfileSchema.get(2).toString)
    Assert.assertEquals("[device_2.sensor_3,INT32]", tsfileSchema.get(3).toString)
    Assert.assertEquals("[device_2.sensor_1,FLOAT]", tsfileSchema.get(4).toString)
    Assert.assertEquals("[device_2.sensor_2,INT32]", tsfileSchema.get(5).toString)
  }

  test("testToSqlValue") {
    val boolField = new Field(TSDataType.BOOLEAN)
    boolField.setBoolV(true)
    val intField = new Field(TSDataType.INT32)
    intField.setIntV(32)
    val longField = new Field(TSDataType.INT64)
    longField.setLongV(64l)
    val floatField = new Field(TSDataType.FLOAT)
    floatField.setFloatV(3.14f)
    val doubleField = new Field(TSDataType.DOUBLE)
    doubleField.setDoubleV(0.618d)
    val stringField = new Field(TSDataType.TEXT)
    stringField.setBinaryV(new Binary("pass"))

    Assert.assertEquals(WideConverter.toSqlValue(boolField), true)
    Assert.assertEquals(WideConverter.toSqlValue(intField), 32)
    Assert.assertEquals(WideConverter.toSqlValue(longField), 64l)
    Assert.assertEquals(WideConverter.toSqlValue(floatField), 3.14f)
    Assert.assertEquals(WideConverter.toSqlValue(doubleField), 0.618d)
    Assert.assertEquals(WideConverter.toSqlValue(stringField), "pass")
  }

  test("testToSparkSqlSchema") {
    val fields: util.ArrayList[Series] = new util.ArrayList[Series]()
    fields.add(new Series("device_1.sensor_3", TSDataType.INT32))
    fields.add(new Series("device_1.sensor_1", TSDataType.FLOAT))
    fields.add(new Series("device_1.sensor_2", TSDataType.INT32))
    fields.add(new Series("device_2.sensor_3", TSDataType.INT32))
    fields.add(new Series("device_2.sensor_1", TSDataType.FLOAT))
    fields.add(new Series("device_2.sensor_2", TSDataType.INT32))

    val sqlSchema = WideConverter.toSqlSchema(fields)

    val expectedFields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    expectedFields.add(StructField(QueryConstant.RESERVED_TIME, LongType, false))
    expectedFields.add(StructField("device_1.sensor_3", IntegerType, true))
    expectedFields.add(StructField("device_1.sensor_1", FloatType, true))
    expectedFields.add(StructField("device_1.sensor_2", IntegerType, true))
    expectedFields.add(StructField("device_2.sensor_3", IntegerType, true))
    expectedFields.add(StructField("device_2.sensor_1", FloatType, true))
    expectedFields.add(StructField("device_2.sensor_2", IntegerType, true))

    Assert.assertEquals(StructType(expectedFields), sqlSchema.get)
  }

  test("prep4requiredSchema1") {
    val in = new HDFSInput(new Path(new URI(tsfilePath1)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata

    val requiredFields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    requiredFields.add(StructField(QueryConstant.RESERVED_TIME, LongType, false))
    requiredFields.add(StructField("device_1.sensor_3", IntegerType, true))
    requiredFields.add(StructField("device_1.sensor_1", FloatType, true))
    requiredFields.add(StructField("device_1.sensor_2", IntegerType, true))
    val requiredSchema = StructType(requiredFields)

    val filteredSchema = WideConverter.prepSchema(requiredSchema, tsFileMetaData, reader)

    Assert.assertEquals(3, filteredSchema.size)
    val fields = filteredSchema.fields
    Assert.assertEquals("StructField(device_1.sensor_3,IntegerType,true)", fields(0).toString)
    Assert.assertEquals("StructField(device_1.sensor_1,FloatType,true)", fields(1).toString)
    Assert.assertEquals("StructField(device_1.sensor_2,IntegerType,true)", fields(2).toString)

    in.close()
  }

  test("prepSchema") {
    val in = new HDFSInput(new Path(new URI(tsfilePath1)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata

    val requiredFields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    requiredFields.add(StructField(QueryConstant.RESERVED_TIME, LongType, false))
    val requiredSchema = StructType(requiredFields)

    val filteredSchema = WideConverter.prepSchema(requiredSchema, tsFileMetaData, reader)

    Assert.assertEquals(6, filteredSchema.size)
    val fields = filteredSchema.fields
    Assert.assertEquals("StructField(device_1.sensor_3,IntegerType,true)", fields(0).toString)
    Assert.assertEquals("StructField(device_1.sensor_1,FloatType,true)", fields(1).toString)
    Assert.assertEquals("StructField(device_1.sensor_2,IntegerType,true)", fields(2).toString)
    Assert.assertEquals("StructField(device_2.sensor_3,IntegerType,true)", fields(3).toString)
    Assert.assertEquals("StructField(device_2.sensor_1,FloatType,true)", fields(4).toString)
    Assert.assertEquals("StructField(device_2.sensor_2,IntegerType,true)", fields(5).toString)

    in.close()
  }

  test("toTsRecord") {
    val fields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    fields.add(StructField(QueryConstant.RESERVED_TIME, LongType, false))
    fields.add(StructField("device_1.sensor_3", IntegerType, true))
    fields.add(StructField("device_1.sensor_1", FloatType, true))
    fields.add(StructField("device_1.sensor_2", IntegerType, true))
    fields.add(StructField("device_2.sensor_3", IntegerType, true))
    fields.add(StructField("device_2.sensor_1", FloatType, true))
    fields.add(StructField("device_2.sensor_2", IntegerType, true))
    val schema = StructType(fields)

    val row: InternalRow = new GenericInternalRow(Array(1L, null, 1.2f, 20, 19, 2.3f, 11))
    val records = WideConverter.toTsRecord(row, schema)

    Assert.assertEquals(2, records.size)
    Assert.assertEquals(1, records(0).time)
    Assert.assertEquals("device_2", records(0).deviceId)
    val dataPoints1 = records(0).dataPointList
    Assert.assertEquals("{measurement id: sensor_3 type: INT32 value: 19 }", dataPoints1.get(0).toString)
    Assert.assertEquals("{measurement id: sensor_1 type: FLOAT value: 2.3 }", dataPoints1.get(1).toString)
    Assert.assertEquals("{measurement id: sensor_2 type: INT32 value: 11 }", dataPoints1.get(2).toString)

    Assert.assertEquals(1, records(1).time)
    Assert.assertEquals("device_1", records(1).deviceId)
    val dataPoints2 = records(1).dataPointList
    Assert.assertEquals("{measurement id: sensor_1 type: FLOAT value: 1.2 }", dataPoints2.get(0).toString)
    Assert.assertEquals("{measurement id: sensor_2 type: INT32 value: 20 }", dataPoints2.get(1).toString)
  }

  test("toQueryExpression") {
    val fields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    fields.add(StructField(QueryConstant.RESERVED_TIME, LongType, false))
    fields.add(StructField("device_1.sensor_1", FloatType, true))
    fields.add(StructField("device_1.sensor_2", IntegerType, true))
    val schema = StructType(fields)

    val ft1 = IsNotNull("time")
    val ft2 = GreaterThan("device_1.sensor_1", 0.0f)
    val ft3 = LessThan("device_1.sensor_2", 22)
    val ft2_3 = Or(ft2, ft3)
    val ft4 = LessThan("time", 4L)
    val filters: Seq[Filter] = Seq(ft1, ft2_3, ft4)

    val expression = WideConverter.toQueryExpression(schema, filters)

    Assert.assertEquals(true, expression.hasQueryFilter)
    Assert.assertEquals(2, expression.getSelectedSeries.size())
    Assert.assertEquals("device_1.sensor_1", expression.getSelectedSeries.get(0).toString)
    Assert.assertEquals("device_1.sensor_2", expression.getSelectedSeries.get(1).toString)
    Assert.assertEquals("[[[device_1.sensor_1:value > 0.0] || [device_1.sensor_2:value < 22]] && [time < 4]]", expression.getExpression.toString)
  }
}
