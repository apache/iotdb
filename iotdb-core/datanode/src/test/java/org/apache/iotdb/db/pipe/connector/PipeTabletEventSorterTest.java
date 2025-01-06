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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.db.pipe.connector.util.PipeTabletEventSorter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipeTabletEventSorterTest {

  @Test
  public void testDeduplicateAndSort() {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet("root.sg.device", schemaList, 30);

    long timestamp = 300;
    for (long i = 0; i < 10; i++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp + i);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, timestamp + i);
      }

      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp - i);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, timestamp - i);
      }

      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, timestamp);
      }
    }

    Set<Integer> indices = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      indices.add((int) tablet.getTimestamp(i));
    }

    Assert.assertFalse(tablet.isSorted());

    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertTrue(tablet.isSorted());

    Assert.assertEquals(indices.size(), tablet.getRowSize());

    final long[] timestamps = Arrays.copyOfRange(tablet.getTimestamps(), 0, tablet.getRowSize());
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[0], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] > timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertTrue((long) tablet.getValue(i, j) > (long) tablet.getValue(i - 1, j));
      }
    }
  }

  @Test
  public void testDeduplicate() {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet("root.sg.device", schemaList, 10);

    long timestamp = 300;
    for (long i = 0; i < 10; i++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, timestamp);
      }
    }

    Set<Integer> indices = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      indices.add((int) tablet.getTimestamp(i));
    }

    Assert.assertTrue(tablet.isSorted());

    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertTrue(tablet.isSorted());

    Assert.assertEquals(indices.size(), tablet.getRowSize());

    final long[] timestamps = Arrays.copyOfRange(tablet.getTimestamps(), 0, tablet.getRowSize());
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[0], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] > timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertTrue((long) tablet.getValue(i, j) > (long) tablet.getValue(i - 1, j));
      }
    }
  }

  @Test
  public void testSort() {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet("root.sg.device", schemaList, 30);

    for (long i = 0; i < 10; i++) {
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, (long) rowIndex + 2);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, (long) rowIndex + 2);
      }

      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, (long) rowIndex);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, (long) rowIndex);
      }

      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, (long) rowIndex - 2);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, (long) rowIndex - 2);
      }
    }

    Set<Integer> indices = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      indices.add((int) tablet.getTimestamp(i));
    }

    Assert.assertFalse(tablet.isSorted());

    long[] timestamps = Arrays.copyOfRange(tablet.getTimestamps(), 0, tablet.getRowSize());
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[0], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] != timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertNotEquals((long) tablet.getValue(i, j), (long) tablet.getValue(i - 1, j));
      }
    }

    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertTrue(tablet.isSorted());

    Assert.assertEquals(indices.size(), tablet.getRowSize());

    timestamps = Arrays.copyOfRange(tablet.getTimestamps(), 0, tablet.getRowSize());
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[0], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] > timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertTrue((long) tablet.getValue(i, j) > (long) tablet.getValue(i - 1, j));
      }
    }
  }
}
