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

package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TabletTest {
  @Test
  public void testSerializationAndDeSerialization() {
    String deviceId = "root.sg";
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));

    int rowSize = 100;
    long[] timestamps = new long[rowSize];
    Object[] values = new Object[2];
    values[0] = new int[rowSize];
    values[1] = new long[rowSize];

    for (int i = 0; i < rowSize; i++) {
      timestamps[i] = i;
      ((int[]) values[0])[i] = 1;
      ((long[]) values[1])[i] = 1;
    }

    Tablet tablet = new Tablet(deviceId, measurementSchemas, timestamps, values, null, rowSize);
    try {
      ByteBuffer byteBuffer = tablet.serialize();
      Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(newTablet, tablet);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
