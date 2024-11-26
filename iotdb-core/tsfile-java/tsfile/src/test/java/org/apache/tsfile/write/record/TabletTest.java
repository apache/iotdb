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

package org.apache.tsfile.write.record;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TabletTest {

  @Test
  public void testAddValue() {
    Tablet tablet =
        new Tablet(
            "root.testsg.d1",
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)));
    tablet.addTimestamp(0, 0);
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, true);
    tablet.addTimestamp(1, 1);
    tablet.addValue(1, 0, false);
    tablet.addValue(1, 1, true);
    tablet.addTimestamp(2, 2);
    tablet.addValue(2, 0, true);

    Assert.assertEquals(tablet.getRowSize(), 3);
    Assert.assertTrue((Boolean) tablet.getValue(0, 0));
    Assert.assertTrue((Boolean) tablet.getValue(0, 1));
    Assert.assertFalse((Boolean) tablet.getValue(1, 0));
    Assert.assertTrue((Boolean) tablet.getValue(1, 1));
    Assert.assertTrue((Boolean) tablet.getValue(2, 0));
    Assert.assertFalse(tablet.bitMaps[0].isMarked(0));
    Assert.assertFalse(tablet.bitMaps[0].isMarked(1));
    Assert.assertFalse(tablet.bitMaps[0].isMarked(2));
    Assert.assertFalse(tablet.bitMaps[1].isMarked(0));
    Assert.assertFalse(tablet.bitMaps[1].isMarked(1));
    Assert.assertTrue(tablet.bitMaps[1].isMarked(2));

    tablet.addTimestamp(9, 9);
    Assert.assertEquals(10, tablet.getRowSize());

    tablet.reset();
    Assert.assertEquals(0, tablet.getRowSize());
    Assert.assertTrue(tablet.bitMaps[0].isAllMarked());
    Assert.assertTrue(tablet.bitMaps[0].isAllMarked());
    Assert.assertTrue(tablet.bitMaps[0].isAllMarked());
  }

  @Test
  public void testSerializationAndDeSerialization() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));

    final int rowSize = 100;
    final long[] timestamps = new long[rowSize];
    final Object[] values = new Object[2];
    values[0] = new int[rowSize];
    values[1] = new long[rowSize];

    for (int i = 0; i < rowSize; i++) {
      timestamps[i] = i;
      ((int[]) values[0])[i] = 1;
      ((long[]) values[1])[i] = 1;
    }

    final Tablet tablet =
        new Tablet(
            deviceId,
            measurementSchemas,
            timestamps,
            values,
            new BitMap[] {new BitMap(1024), new BitMap(1024)},
            rowSize);
    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertEquals(tablet.getValue(i, j), newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializationAndDeSerializationWithMoreData() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s6", TSDataType.STRING, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s7", TSDataType.BLOB, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s8", TSDataType.TIMESTAMP, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s9", TSDataType.DATE, TSEncoding.PLAIN));

    final int rowSize = 1000;
    final Tablet tablet = new Tablet(deviceId, measurementSchemas);
    tablet.setRowSize(rowSize);
    tablet.initBitMaps();
    for (int i = 0; i < rowSize - 1; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue(measurementSchemas.get(0).getMeasurementName(), i, i);
      tablet.addValue(measurementSchemas.get(1).getMeasurementName(), i, (long) i);
      tablet.addValue(measurementSchemas.get(2).getMeasurementName(), i, (float) i);
      tablet.addValue(measurementSchemas.get(3).getMeasurementName(), i, (double) i);
      tablet.addValue(measurementSchemas.get(4).getMeasurementName(), i, (i % 2) == 0);
      tablet.addValue(measurementSchemas.get(5).getMeasurementName(), i, String.valueOf(i));
      tablet.addValue(measurementSchemas.get(6).getMeasurementName(), i, String.valueOf(i));
      tablet.addValue(
          measurementSchemas.get(7).getMeasurementName(),
          i,
          new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET));
      tablet.addValue(measurementSchemas.get(8).getMeasurementName(), i, (long) i);
      tablet.addValue(
          measurementSchemas.get(9).getMeasurementName(),
          i,
          LocalDate.of(2000 + i, i / 100 + 1, i / 100 + 1));

      tablet.bitMaps[i % measurementSchemas.size()].mark(i);
    }

    // Test add null
    tablet.addTimestamp(rowSize - 1, rowSize - 1);
    tablet.addValue(measurementSchemas.get(0).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(1).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(2).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(3).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(4).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(5).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(6).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(7).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(8).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(9).getMeasurementName(), rowSize - 1, null);

    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertEquals(tablet.getValue(i, j), newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializationAndDeSerializationNull() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s6", TSDataType.STRING, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s7", TSDataType.BLOB, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s8", TSDataType.TIMESTAMP, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s9", TSDataType.DATE, TSEncoding.PLAIN));

    final int rowSize = 1000;
    final Tablet tablet = new Tablet(deviceId, measurementSchemas);
    tablet.setRowSize(rowSize);
    tablet.initBitMaps();
    for (int i = 0; i < rowSize; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue(measurementSchemas.get(0).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(1).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(2).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(3).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(4).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(5).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(6).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(7).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(8).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(9).getMeasurementName(), i, null);
    }

    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertNull(tablet.getValue(i, j));
          assertNull(newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
