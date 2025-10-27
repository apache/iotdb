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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipeTabletEventSorterTest {

  @Test
  public void testTreeModelDeduplicateAndSort() {
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

    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

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
  public void testTreeModelDeduplicate() {
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

    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    Assert.assertTrue(tablet.isSorted());

    Assert.assertEquals(indices.size(), tablet.getRowSize());

    final long[] timestamps = Arrays.copyOfRange(tablet.getTimestamps(), 0, tablet.getRowSize());
    Assert.assertEquals(timestamps[0] + 8, ((long[]) tablet.getValues()[0])[0]);
    for (int i = 1; i < 3; ++i) {
      Assert.assertEquals(timestamps[0] + 9, ((long[]) tablet.getValues()[i])[0]);
    }
  }

  @Test
  public void testTreeModelSort() {
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
          timestamps, Arrays.copyOfRange((long[]) tablet.getValues()[0], 0, tablet.getRowSize()));
    }

    for (int i = 1; i < tablet.getRowSize(); ++i) {
      Assert.assertTrue(timestamps[i] != timestamps[i - 1]);
      for (int j = 0; j < 3; ++j) {
        Assert.assertNotEquals((long) tablet.getValue(i, j), (long) tablet.getValue(i - 1, j));
      }
    }

    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

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

  @Test
  public void testTableModelDeduplicateAndSort() {
    doTableModelTest(true, true);
  }

  @Test
  public void testTableModelDeduplicate() {
    doTableModelTest(true, false);
  }

  @Test
  public void testTableModelSort() {
    doTableModelTest(false, true);
  }

  @Test
  public void testTableModelSort1() {
    doTableModelTest1();
  }

  public void doTableModelTest(final boolean hasDuplicates, final boolean isUnSorted) {
    final Tablet tablet = generateTablet("test", 10, hasDuplicates, isUnSorted);
    new PipeTableModelTabletEventSorter(tablet).sortAndDeduplicateByDevIdTimestamp();
    long[] timestamps = tablet.getTimestamps();
    for (int i = 1; i < tablet.getRowSize(); i++) {
      long time = timestamps[i];
      Assert.assertTrue(time > timestamps[i - 1]);
      Assert.assertEquals(
          tablet.getValue(i, 0),
          new Binary(String.valueOf(i / 100).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(tablet.getValue(i, 1), (long) i);
      Assert.assertEquals(tablet.getValue(i, 2), i * 1.0f);
      Assert.assertEquals(
          tablet.getValue(i, 3), new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(tablet.getValue(i, 4), (long) i);
      Assert.assertEquals(tablet.getValue(i, 5), i);
      Assert.assertEquals(tablet.getValue(i, 6), i * 0.1);
      Assert.assertEquals(tablet.getValue(i, 7), getDate(i));
      Assert.assertEquals(
          tablet.getValue(i, 8), new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
    }
  }

  public void doTableModelTest1() {
    final Tablet tablet = generateTablet("test", 10, false, true);
    new PipeTableModelTabletEventSorter(tablet).sortByTimestampIfNecessary();
    long[] timestamps = tablet.getTimestamps();
    for (int i = 1; i < tablet.getRowSize(); i++) {
      long time = timestamps[i];
      Assert.assertTrue(time > timestamps[i - 1]);
      Assert.assertEquals(
          tablet.getValue(i, 0),
          new Binary(String.valueOf(i / 100).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(tablet.getValue(i, 1), (long) i);
      Assert.assertEquals(tablet.getValue(i, 2), i * 1.0f);
      Assert.assertEquals(
          tablet.getValue(i, 3), new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(tablet.getValue(i, 4), (long) i);
      Assert.assertEquals(tablet.getValue(i, 5), i);
      Assert.assertEquals(tablet.getValue(i, 6), i * 0.1);
      Assert.assertEquals(tablet.getValue(i, 7), getDate(i));
      Assert.assertEquals(
          tablet.getValue(i, 8), new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
    }
  }

  static Tablet generateTablet(
      final String tableName,
      final int deviceIDNum,
      final boolean hasDuplicates,
      final boolean isUnSorted) {
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s3", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s4", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s5", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s6", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s7", TSDataType.DATE));
    schemaList.add(new MeasurementSchema("s8", TSDataType.TEXT));

    final List<ColumnCategory> columnTypes =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    Tablet tablet =
        new Tablet(
            tableName,
            IMeasurementSchema.getMeasurementNameList(schemaList),
            IMeasurementSchema.getDataTypeList(schemaList),
            columnTypes,
            deviceIDNum * 1000);
    tablet.initBitMaps();

    // s2 float, s3 string, s4 timestamp, s5 int32, s6 double, s7 date, s8 text
    int rowIndex = 0;

    for (long row = 0; row < deviceIDNum; row++) {
      for (int i = 0; i < (isUnSorted ? 50 : 100); i++) {

        final long value;
        if (isUnSorted) {
          value = (row + 1) * 100 - i - 1;
        } else {
          value = (row) * 100 + i;
        }
        for (int j = 0; j < 10; j++) {
          tablet.addTimestamp(rowIndex, value);
          tablet.addValue(
              "s0", rowIndex, new Binary(String.valueOf(row).getBytes(StandardCharsets.UTF_8)));
          tablet.addValue("s1", rowIndex, hasDuplicates && j == 0 ? null : value);
          tablet.addValue("s2", rowIndex, hasDuplicates && j == 0 ? null : (value * 1.0f));
          tablet.addValue(
              "s3",
              rowIndex,
              hasDuplicates && j == 0
                  ? null
                  : new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
          tablet.addValue("s4", rowIndex, hasDuplicates && j == 0 ? null : value);
          tablet.addValue("s5", rowIndex, hasDuplicates && j == 0 ? null : (int) value);
          tablet.addValue("s6", rowIndex, hasDuplicates && j == 0 ? null : value * 0.1);
          tablet.addValue("s7", rowIndex, hasDuplicates && j == 0 ? null : getDate((int) value));
          tablet.addValue(
              "s8",
              rowIndex,
              hasDuplicates && j == 0
                  ? null
                  : new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
          rowIndex++;
          tablet.setRowSize(rowIndex);
          if (!hasDuplicates) {
            break;
          }
        }
      }
    }
    if (!isUnSorted) {
      return tablet;
    }
    for (long row = 0; row < deviceIDNum; row++) {
      for (int i = 50; i < 100; i++) {

        final long value;
        value = (row + 1) * 100 - i - 1;

        for (int j = 0; j < 10; j++) {
          tablet.addTimestamp(rowIndex, value);
          tablet.addValue(
              "s0", rowIndex, new Binary(String.valueOf(row).getBytes(StandardCharsets.UTF_8)));
          tablet.addValue("s1", rowIndex, value);
          tablet.addValue("s2", rowIndex, (value * 1.0f));
          tablet.addValue(
              "s3", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
          tablet.addValue("s4", rowIndex, value);
          tablet.addValue("s5", rowIndex, (int) value);
          tablet.addValue("s6", rowIndex, value * 0.1);
          tablet.addValue("s7", rowIndex, getDate((int) value));
          tablet.addValue(
              "s8", rowIndex, new Binary(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
          rowIndex++;
          tablet.setRowSize(rowIndex);
          if (!hasDuplicates) {
            break;
          }
        }
      }
    }
    return tablet;
  }

  public static LocalDate getDate(final int value) {
    Date date = new Date(value);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return DateUtils.parseIntToLocalDate(
          DateUtils.parseDateExpressionToInt(dateFormat.format(date)));
    } catch (Exception e) {
      return DateUtils.parseIntToLocalDate(DateUtils.parseDateExpressionToInt("1970-01-01"));
    }
  }
}
