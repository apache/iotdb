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
package cn.edu.tsinghua.tsfile

import java.io.File
import java.util

import org.apache.iotdb.tsfile.Converter
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.read.common.Field
import org.apache.iotdb.tsfile.tool.TsFileWrite
import org.apache.iotdb.tsfile.utils.Binary
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ConverterTest extends FunSuite with BeforeAndAfterAll {
  private val tsfilePath: String = "../spark/src/test/resources/tsfile/test.tsfile"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    new TsFileWrite().create1(tsfilePath)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    val file = new File(tsfilePath)
    file.delete()
  }

  test("testToSparkSqlSchema") {
    val fields: util.ArrayList[MeasurementSchema] = new util.ArrayList[MeasurementSchema]()
    fields.add(new MeasurementSchema("device_1.sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF))
    fields.add(new MeasurementSchema("device_1.sensor_1", TSDataType.FLOAT, TSEncoding.RLE))
    fields.add(new MeasurementSchema("device_1.sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF))
    fields.add(new MeasurementSchema("device_2.sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF))
    fields.add(new MeasurementSchema("device_2.sensor_1", TSDataType.FLOAT, TSEncoding.RLE))
    fields.add(new MeasurementSchema("device_2.sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF))

    val sqlSchema = Converter.toSqlSchema(fields)

    val expectedFields: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    expectedFields.add(new StructField(QueryConstant.RESERVED_TIME, LongType, false))
    expectedFields.add(new StructField("device_1.sensor_3", IntegerType, true))
    expectedFields.add(new StructField("device_1.sensor_1", FloatType, true))
    expectedFields.add(new StructField("device_1.sensor_2", IntegerType, true))
    expectedFields.add(new StructField("device_2.sensor_3", IntegerType, true))
    expectedFields.add(new StructField("device_2.sensor_1", FloatType, true))
    expectedFields.add(new StructField("device_2.sensor_2", IntegerType, true))

    Assert.assertEquals(StructType(expectedFields), sqlSchema.get)
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

    Assert.assertEquals(Converter.toSqlValue(boolField), true)
    Assert.assertEquals(Converter.toSqlValue(intField), 32)
    Assert.assertEquals(Converter.toSqlValue(longField), 64l)
    Assert.assertEquals(Converter.toSqlValue(floatField), 3.14f)
    Assert.assertEquals(Converter.toSqlValue(doubleField), 0.618d)
    Assert.assertEquals(Converter.toSqlValue(stringField), "pass")
  }
}
