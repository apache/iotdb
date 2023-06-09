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
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    Tablet tablet =
        new Tablet(
            deviceId,
            measurementSchemas,
            timestamps,
            values,
            new BitMap[] {new BitMap(1024), new BitMap(1024)},
            rowSize);
    try {
      ByteBuffer byteBuffer = tablet.serialize();
      Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(newTablet, tablet);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializationAndDeSerializationWithMoreData() {
    String deviceId = "root.sg";
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));

    int rowSize = 1000;
    Tablet tablet = new Tablet(deviceId, measurementSchemas);
    tablet.rowSize = rowSize;
    tablet.initBitMaps();
    for (int i = 0; i < rowSize; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue(measurementSchemas.get(0).getMeasurementId(), i, i);
      tablet.addValue(measurementSchemas.get(1).getMeasurementId(), i, (long) i);
      tablet.addValue(measurementSchemas.get(2).getMeasurementId(), i, (float) i);
      tablet.addValue(measurementSchemas.get(3).getMeasurementId(), i, (double) i);
      tablet.addValue(measurementSchemas.get(4).getMeasurementId(), i, (i % 2) == 0);
      tablet.addValue(measurementSchemas.get(5).getMeasurementId(), i, String.valueOf(i));

      tablet.bitMaps[i % measurementSchemas.size()].mark(i);
    }

    try {
      ByteBuffer byteBuffer = tablet.serialize();
      Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertTrue(newTablet.equals(tablet));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
