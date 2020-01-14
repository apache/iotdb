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

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

public class SchemaBuilderTest {

  @Test
  public void testJsonConverter() {

    Map<String, String> props = new HashMap<>();
    props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
    Schema schema = new Schema();
    schema.registerTimeseries(new Path("d1", "s4"),
        new TimeseriesSchema("s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, props));
    schema.registerTimeseries(new Path("d1", "s5"),
        new TimeseriesSchema("s5", TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED, null));

    Collection<TimeseriesSchema> timeseries = schema.getTimeseriesSchemaMap().values();
    String[] tsDesStrings = { "[s4,DOUBLE,RLE,{max_point_number=3},SNAPPY]", "[s5,INT32,TS_2DIFF,{},UNCOMPRESSED]" };
    int i = 0;
    for (TimeseriesSchema desc : timeseries) {
      assertEquals(tsDesStrings[i++], desc.toString());
    }
  }
}
