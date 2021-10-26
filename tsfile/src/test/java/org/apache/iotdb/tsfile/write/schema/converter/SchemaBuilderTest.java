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
package org.apache.iotdb.tsfile.write.schema.converter;

import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SchemaBuilderTest {

  @Test
  public void testJsonConverter1() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    schema.registerTimeseries(
        new Path("d1", "s4"),
        new UnaryMeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    schema.registerTimeseries(
        new Path("d1", "s5"),
        new UnaryMeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));

    Collection<IMeasurementSchema> timeseries = schema.getRegisteredTimeseriesMap().values();
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]", "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]"
    };
    int i = 0;
    for (IMeasurementSchema desc : timeseries) {
      assertEquals(tsDesStrings[i++], desc.toString());
    }
  }

  @Test
  public void testJsonConverter2() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    Map<String, IMeasurementSchema> template = new HashMap<>();
    template.put(
        "s4",
        new UnaryMeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    template.put(
        "s5",
        new UnaryMeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));
    schema.registerSchemaTemplate("template1", template);
    schema.registerDevice("d1", "template1");

    Collection<IMeasurementSchema> timeseries = schema.getRegisteredTimeseriesMap().values();
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]", "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]"
    };
    int i = 0;
    for (IMeasurementSchema desc : timeseries) {
      assertEquals(tsDesStrings[i++], desc.toString());
    }
  }

  @Test
  public void testJsonConverter3() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    Map<String, IMeasurementSchema> template = new HashMap<>();
    template.put(
        "s4",
        new UnaryMeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    template.put(
        "s5",
        new UnaryMeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));
    schema.registerSchemaTemplate("template1", template);

    schema.extendTemplate(
        "template1",
        new UnaryMeasurementSchema(
            "s6", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY, props));

    schema.registerDevice("d1", "template1");

    Collection<IMeasurementSchema> timeseries = schema.getRegisteredTimeseriesMap().values();
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]",
      "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]",
      "[s6,INT64,RLE,{max_point_number=3},SNAPPY]"
    };
    int i = 0;
    for (IMeasurementSchema desc : timeseries) {
      assertEquals(tsDesStrings[i++], desc.toString());
    }
  }
}
