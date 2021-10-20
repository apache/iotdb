/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink;

import org.apache.iotdb.flink.options.IoTDBSinkOptions;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DefaultIoTSerializationSchemaTest {

  @Test
  public void serialize() {
    IoTDBSinkOptions options = new IoTDBSinkOptions();
    options.setTimeseriesOptionList(
        Lists.newArrayList(new IoTDBSinkOptions.TimeseriesOption("root.sg.D01.temperature")));
    DefaultIoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();

    Map<String, String> tuple = new HashMap();
    tuple.put("device", "root.sg.D01");
    tuple.put("timestamp", "1581861293000");
    tuple.put("measurements", "temperature");
    tuple.put("types", "DOUBLE");
    tuple.put("values", "36.5");

    Event event = serializationSchema.serialize(tuple);
    assertEquals(tuple.get("device"), event.getDevice());
    assertEquals(tuple.get("timestamp"), String.valueOf(event.getTimestamp()));
    assertEquals(tuple.get("measurements"), event.getMeasurements().get(0));
    assertEquals(tuple.get("types"), event.getTypes().get(0).toString());
    assertEquals(tuple.get("values"), String.valueOf(event.getValues().get(0)));
  }
}
