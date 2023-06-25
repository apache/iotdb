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
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SchemaBuilderTest {

  @Test
  public void testJsonConverter1() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));

    List<IMeasurementSchema> timeseries = new ArrayList<>();
    schema
        .getRegisteredTimeseriesMap()
        .values()
        .forEach(
            mg -> {
              timeseries.addAll(mg.getMeasurementSchemaMap().values());
            });
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]", "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]"
    };
    Set<String> expected = new HashSet<String>(Arrays.asList(tsDesStrings));
    List<String> actual = new ArrayList<String>();
    for (IMeasurementSchema desc : timeseries) {
      actual.add(desc.toString());
    }
    assertEquals(expected.size(), actual.size());
    assertEquals(expected, new HashSet<String>(actual));
  }

  @Test
  public void testJsonConverter2() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    Map<String, MeasurementSchema> template = new HashMap<>();
    template.put(
        "s4",
        new MeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    template.put(
        "s5",
        new MeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));
    MeasurementGroup measurementGroup = new MeasurementGroup(false, template);
    schema.registerSchemaTemplate("template1", measurementGroup);
    schema.registerDevice("d1", "template1");

    List<IMeasurementSchema> timeseries = new ArrayList<>();
    schema
        .getRegisteredTimeseriesMap()
        .values()
        .forEach(
            mg -> {
              timeseries.addAll(mg.getMeasurementSchemaMap().values());
            });
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]", "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]"
    };
    Set<String> expected = new HashSet<String>(Arrays.asList(tsDesStrings));
    List<String> actual = new ArrayList<String>();
    for (IMeasurementSchema desc : timeseries) {
      actual.add(desc.toString());
    }
    assertEquals(expected.size(), actual.size());
    assertEquals(expected, new HashSet<String>(actual));
  }

  @Test
  public void testJsonConverter3() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    Map<String, MeasurementSchema> template = new HashMap<>();
    template.put(
        "s4",
        new MeasurementSchema(
            "s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    template.put(
        "s5",
        new MeasurementSchema(
            "s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));
    MeasurementGroup measurementGroup = new MeasurementGroup(false, template);
    schema.registerSchemaTemplate("template1", measurementGroup);

    schema.extendTemplate(
        "template1",
        new MeasurementSchema(
            "s6", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY, props));

    schema.registerDevice("d1", "template1");

    List<IMeasurementSchema> timeseries = new ArrayList<>();
    schema
        .getRegisteredTimeseriesMap()
        .values()
        .forEach(
            mg -> {
              timeseries.addAll(mg.getMeasurementSchemaMap().values());
            });
    String[] tsDesStrings = {
      "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]",
      "[s5,INT32,TS_2DIFF,,UNCOMPRESSED]",
      "[s6,INT64,RLE,{max_point_number=3},SNAPPY]"
    };
    Set<String> expected = new HashSet<String>(Arrays.asList(tsDesStrings));
    List<String> actual = new ArrayList<String>();
    for (IMeasurementSchema desc : timeseries) {
      actual.add(desc.toString());
    }
    assertEquals(expected.size(), actual.size());
    assertEquals(expected, new HashSet<String>(actual));
  }
}
