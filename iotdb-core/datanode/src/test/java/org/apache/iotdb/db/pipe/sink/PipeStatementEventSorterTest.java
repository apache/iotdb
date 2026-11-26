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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTableModelTabletEventSorter;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipeStatementEventSorterTest {

  @Test
  public void testTreeModelDeduplicateAndSort() throws Exception {
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

    // Convert Tablet to Statement
    InsertTabletStatement statement = new InsertTabletStatement(tablet, true, null);

    // Sort using Statement
    new PipeTreeModelTabletEventSorter(statement).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertEquals(indices.size(), statement.getRowCount());

    final long[] timestamps = Arrays.copyOfRange(statement.getTimes(), 0, statement.getRowCount());
    final Object[] columns = statement.getColumns();
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) columns[i], 0, statement.getRowCount()));
    }

    for (int i = 1; i < statement.getRowCount(); ++i) {
      Assert.assertTrue(timestamps[i] > timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertTrue(((long[]) columns[j])[i] > ((long[]) columns[j])[i - 1]);
      }
    }
  }

  @Test
  public void testTreeModelDeduplicate() throws Exception {
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    final Tablet tablet = new Tablet("root.sg.device", schemaList, 10);

    final long timestamp = 300;
    for (long i = 0; i < 10; i++) {
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(
            schemaList.get(s).getMeasurementName(),
            rowIndex,
            (i + s) % 3 != 0 ? timestamp + i : null);
      }
    }

    final Set<Integer> indices = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      indices.add((int) tablet.getTimestamp(i));
    }

    Assert.assertTrue(tablet.isSorted());

    // Convert Tablet to Statement
    InsertTabletStatement statement = new InsertTabletStatement(tablet, true, null);

    // Sort using Statement
    new PipeTreeModelTabletEventSorter(statement).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertEquals(indices.size(), statement.getRowCount());

    final long[] timestamps = Arrays.copyOfRange(statement.getTimes(), 0, statement.getRowCount());
    final Object[] columns = statement.getColumns();
    Assert.assertEquals(timestamps[0] + 8, ((long[]) columns[0])[0]);
    for (int i = 1; i < 3; ++i) {
      Assert.assertEquals(timestamps[0] + 9, ((long[]) columns[i])[0]);
    }
  }

  @Test
  public void testTreeModelSort() throws Exception {
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
      tablet.addTimestamp(rowIndex, rowIndex);
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
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[i], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] != timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertNotEquals((long) tablet.getValue(i, j), (long) tablet.getValue(i - 1, j));
      }
    }

    // Convert Tablet to Statement
    InsertTabletStatement statement = new InsertTabletStatement(tablet, true, null);

    // Sort using Statement
    new PipeTreeModelTabletEventSorter(statement).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertEquals(indices.size(), statement.getRowCount());

    timestamps = Arrays.copyOfRange(statement.getTimes(), 0, statement.getRowCount());
    final Object[] columns = statement.getColumns();
    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(
          timestamps, Arrays.copyOfRange((long[]) columns[i], 0, statement.getRowCount()));
    }

    for (int i = 1; i < statement.getRowCount(); ++i) {
      Assert.assertTrue(timestamps[i] > timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertTrue(((long[]) columns[j])[i] > ((long[]) columns[j])[i - 1]);
      }
    }
  }

  @Test
  public void testTableModelDeduplicateAndSort() throws Exception {
    doTableModelTest(true, true);
  }

  @Test
  public void testTableModelDeduplicate() throws Exception {
    doTableModelTest(true, false);
  }

  @Test
  public void testTableModelSort() throws Exception {
    doTableModelTest(false, true);
  }

  @Test
  public void testTableModelSort1() throws Exception {
    doTableModelTest1();
  }

  public void doTableModelTest(final boolean hasDuplicates, final boolean isUnSorted)
      throws Exception {
    final Tablet tablet =
        PipeTabletEventSorterTest.generateTablet("test", 10, hasDuplicates, isUnSorted);

    // Convert Tablet to Statement
    InsertTabletStatement statement = new InsertTabletStatement(tablet, false, "test_db");

    // Sort using Statement
    new PipeTableModelTabletEventSorter(statement).sortAndDeduplicateByDevIdTimestamp();

    long[] timestamps = statement.getTimes();
    final Object[] columns = statement.getColumns();
    for (int i = 1; i < statement.getRowCount(); i++) {
      long time = timestamps[i];
      Assert.assertTrue(time > timestamps[i - 1]);
      Assert.assertEquals(
          ((Binary[]) columns[0])[i],
          new Binary(String.valueOf(i / 100).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(((long[]) columns[1])[i], (long) i);
      Assert.assertEquals(((float[]) columns[2])[i], i * 1.0f, 0.001f);
      Assert.assertEquals(
          ((Binary[]) columns[3])[i],
          new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(((long[]) columns[4])[i], (long) i);
      Assert.assertEquals(((int[]) columns[5])[i], i);
      Assert.assertEquals(((double[]) columns[6])[i], i * 0.1, 0.0001);
      // DATE is stored as int[] in Statement, not LocalDate[]
      LocalDate expectedDate = PipeTabletEventSorterTest.getDate(i);
      int expectedDateInt =
          org.apache.tsfile.utils.DateUtils.parseDateExpressionToInt(expectedDate);
      Assert.assertEquals(((int[]) columns[7])[i], expectedDateInt);
      Assert.assertEquals(
          ((Binary[]) columns[8])[i],
          new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
    }
  }

  public void doTableModelTest1() throws Exception {
    final Tablet tablet = PipeTabletEventSorterTest.generateTablet("test", 10, false, true);

    // Convert Tablet to Statement
    InsertTabletStatement statement = new InsertTabletStatement(tablet, false, "test_db");

    // Sort using Statement
    new PipeTableModelTabletEventSorter(statement).sortByTimestampIfNecessary();

    long[] timestamps = statement.getTimes();
    final Object[] columns = statement.getColumns();
    for (int i = 1; i < statement.getRowCount(); i++) {
      long time = timestamps[i];
      Assert.assertTrue(time > timestamps[i - 1]);
      Assert.assertEquals(
          ((Binary[]) columns[0])[i],
          new Binary(String.valueOf(i / 100).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(((long[]) columns[1])[i], (long) i);
      Assert.assertEquals(((float[]) columns[2])[i], i * 1.0f, 0.001f);
      Assert.assertEquals(
          ((Binary[]) columns[3])[i],
          new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(((long[]) columns[4])[i], (long) i);
      Assert.assertEquals(((int[]) columns[5])[i], i);
      Assert.assertEquals(((double[]) columns[6])[i], i * 0.1, 0.0001);
      // DATE is stored as int[] in Statement, not LocalDate[]
      LocalDate expectedDate = PipeTabletEventSorterTest.getDate(i);
      int expectedDateInt =
          org.apache.tsfile.utils.DateUtils.parseDateExpressionToInt(expectedDate);
      Assert.assertEquals(((int[]) columns[7])[i], expectedDateInt);
      Assert.assertEquals(
          ((Binary[]) columns[8])[i],
          new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
    }
  }
}
