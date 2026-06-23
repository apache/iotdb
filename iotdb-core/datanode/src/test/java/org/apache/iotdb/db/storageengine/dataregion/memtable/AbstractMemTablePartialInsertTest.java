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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests that verify the fix for the negative {@code pointsInserted} bug when a partial insert
 * contains failed measurements (i.e. {@code measurements[i] == null}).
 *
 * <p>Before the fix:
 *
 * <ul>
 *   <li>{@code insertAlignedRow}: a failed measurement whose value slot is also {@code null} was
 *       incorrectly counted as a {@code nullPoint}, making {@code pointsInserted} go negative.
 *   <li>{@code computeTabletNullPointsNumber}: failed measurements were not skipped, so their
 *       bitmap marks were counted, again producing a negative {@code pointsInserted}.
 * </ul>
 *
 * <p>After the fix both paths skip failed measurements, so {@code pointsInserted >= 0} always.
 */
public class AbstractMemTablePartialInsertTest {

  private PrimitiveMemTable memTable;
  private static boolean prevEnableNullValueInWriteThroughputMetric;

  @Before
  public void setUp() {
    memTable = new PrimitiveMemTable("root.sg", "0");
    prevEnableNullValueInWriteThroughputMetric =
        IoTDBDescriptor.getInstance().getConfig().isIncludeNullValueInWriteThroughputMetric();
    IoTDBDescriptor.getInstance().getConfig().setIncludeNullValueInWriteThroughputMetric(false);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setIncludeNullValueInWriteThroughputMetric(prevEnableNullValueInWriteThroughputMetric);
  }

  // =========================================================================
  // insertAlignedRow – failed measurement must not be counted as nullPoint
  // =========================================================================

  /** All measurements succeed, no null values → pointsInserted == total measurements (3). */
  @Test
  public void testInsertAlignedRow_noFailure_noNullValue_pointsInsertedEqualsTotal()
      throws IllegalPathException {
    // 3 measurements, all valid, all have values
    // formula: getMeasurementColumnCnt(3) - failedNum(0) - nullPoints(0) = 3
    InsertRowNode node =
        buildAlignedInsertRowNode(
            new String[] {"s0", "s1", "s2"}, new Object[] {1, 2, 3}, -1 /* no failure */);

    int points = memTable.insertAlignedRow(node);

    assertEquals(3, points);
    assertEquals(3, memTable.getTotalPointsNum());
  }

  /**
   * One measurement fails (partial insert). The failed slot has measurements[i]==null and
   * values[i]==null. Before the fix this null value was counted as a nullPoint, making
   * pointsInserted negative. After the fix it is skipped.
   *
   * <p>formula: getMeasurementColumnCnt(2) - failedNum(1) - nullPoints(0) = 1
   */
  @Test
  public void testInsertAlignedRow_oneFailedMeasurement_pointsInsertedNotNegative()
      throws IllegalPathException {
    // 2 measurements, first one fails
    InsertRowNode node =
        buildAlignedInsertRowNode(
            new String[] {"s0", "s1"}, new Object[] {1, 2}, 0 /* mark index 0 as failed */);

    int points = memTable.insertAlignedRow(node);

    assertEquals(1, points);
    assertEquals(1, memTable.getTotalPointsNum());
  }

  @Test
  public void testInsertAlignedRow_markedFailedMeasurementOnly_pointsInsertedMatchesWrittenPoints()
      throws IllegalPathException {
    InsertRowNode node =
        buildAlignedInsertRowNode(
            new String[] {"s0", "s1"}, new Object[] {1, 2}, -1 /* no failure */);
    node.markFailedMeasurement(0);

    int points = memTable.insertAlignedRow(node);

    assertEquals(1, points);
    assertEquals(1, memTable.getTotalPointsNum());
  }

  /** All measurements fail → insertAlignedRow returns 0 early (schemaList is empty). */
  @Test
  public void testInsertAlignedRow_allMeasurementsFailed_pointsInsertedIsZero()
      throws IllegalPathException {
    InsertRowNode node =
        buildAlignedInsertRowNode(
            new String[] {"s0", "s1"},
            new Object[] {1, 2L},
            -1 /* mark all failed manually below */);
    // mark both as failed
    node.markFailedMeasurement(0);
    node.markFailedMeasurement(1);
    node.setFailedMeasurementNumber(2);

    int points = memTable.insertAlignedRow(node);

    assertEquals(0, points);
    assertEquals(0, memTable.getTotalPointsNum());
  }

  @Test
  public void testInsertAlignedRow_tableNonFieldAndFailedMeasurementsNotCountedAsSeries()
      throws IllegalPathException {
    InsertRowNode node =
        buildAlignedInsertRowNode(
            new String[] {"tag1", "attr1", "s0", "s1"},
            new Object[] {1, 2, 3, 4},
            -1 /* no failure */);
    node.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD
        });
    node.markFailedMeasurement(3);

    int points = memTable.insertAlignedRow(node);

    assertEquals(1, points);
    assertEquals(1, memTable.getTotalPointsNum());
    assertEquals(1, memTable.getSeriesNumber());
  }

  @Test
  public void testInsertRow_tableNonFieldAndFailedMeasurementsNotCountedAsSeries()
      throws IllegalPathException {
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("test"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"tag1", "attr1", "s0", "s1"},
            new TSDataType[] {
              TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32
            },
            new MeasurementSchema[] {
              new MeasurementSchema("tag1", TSDataType.INT32),
              new MeasurementSchema("attr1", TSDataType.INT32),
              new MeasurementSchema("s0", TSDataType.INT32),
              new MeasurementSchema("s1", TSDataType.INT32)
            },
            1L,
            new Object[] {1, 2, 3, 4},
            false);
    node.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD
        });
    node.markFailedMeasurement(3);

    int points = memTable.insert(node);

    assertEquals(1, points);
    assertEquals(1, memTable.getTotalPointsNum());
    assertEquals(1, memTable.getSeriesNumber());
  }

  // =========================================================================
  // insertTablet – failed measurement must be skipped in null-point counting
  // =========================================================================

  /** Normal tablet insert with no failures and no null values → pointsInserted == cols * rows. */
  @Test
  public void testInsertTablet_noFailure_noNullBits_pointsInsertedEqualsColsTimesRows()
      throws IllegalPathException, WriteProcessException {
    // formula: (dataTypes.length(2) - failedNum(0)) * rows(3) - nullPoints(0) = 6
    InsertTabletNode node =
        buildInsertTabletNode(
            new String[] {"s0", "s1"}, 3, null /* no bitmaps */, -1 /* no failure */);

    int points = memTable.insertTablet(node, 0, 3);

    assertEquals(6, points);
    assertEquals(6, memTable.getTotalPointsNum());
  }

  /**
   * One measurement fails. Before the fix, if the failed column's bitmap had marks, those marks
   * were counted as null points, making pointsInserted negative. After the fix the failed column is
   * skipped entirely.
   *
   * <p>formula: (dataTypes.length(2) - failedNum(1)) * rows(3) - nullPoints(0, col-0 skipped) = 3
   */
  @Test
  public void testInsertTablet_oneFailedMeasurement_withBitmap_pointsInsertedNotNegative()
      throws IllegalPathException, WriteProcessException {
    int rowCount = 3;
    // bitmap for column 0: all rows marked as null
    BitMap[] bitMaps = new BitMap[2];
    bitMaps[0] = new BitMap(rowCount);
    bitMaps[0].markAll();
    bitMaps[1] = null;

    // mark column 0 as failed (partial insert)
    InsertTabletNode node =
        buildInsertTabletNode(
            new String[] {"s0", "s1"}, rowCount, bitMaps, 0 /* mark index 0 as failed */);

    int points = memTable.insertTablet(node, 0, rowCount);

    assertEquals(3, points);
    assertEquals(3, memTable.getTotalPointsNum());
  }

  @Test
  public void testInsertTablet_markedFailedMeasurementOnly_pointsInsertedMatchesWrittenPoints()
      throws IllegalPathException, WriteProcessException {
    int rowCount = 3;
    InsertTabletNode node =
        buildInsertTabletNode(new String[] {"s0", "s1"}, rowCount, null, -1 /* no failure */);
    node.markFailedMeasurement(0);

    int points = memTable.insertTablet(node, 0, rowCount);

    assertEquals(3, points);
    assertEquals(3, memTable.getTotalPointsNum());
  }

  @Test
  public void testInsertTablet_tableNonFieldAndFailedMeasurementsNotCountedAsSeries()
      throws IllegalPathException, WriteProcessException {
    int rowCount = 3;
    InsertTabletNode node =
        buildInsertTabletNode(
            new String[] {"tag1", "attr1", "s0", "s1"}, rowCount, null, -1 /* no failure */);
    node.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD
        });
    node.markFailedMeasurement(3);

    int points = memTable.insertTablet(node, 0, rowCount);

    assertEquals(rowCount, points);
    assertEquals(rowCount, memTable.getTotalPointsNum());
    assertEquals(1, memTable.getSeriesNumber());
  }

  /** All measurements fail → pointsInserted == 0. formula: (2-2)*3 - 0 = 0 */
  @Test
  public void testInsertTablet_allMeasurementsFailed_pointsInsertedIsZero()
      throws IllegalPathException, WriteProcessException {
    int rowCount = 3;
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"}, rowCount, null, -1);
    node.markFailedMeasurement(0);
    node.markFailedMeasurement(1);
    node.setFailedMeasurementNumber(2);

    int points = memTable.insertTablet(node, 0, rowCount);

    assertEquals(0, points);
    assertEquals(0, memTable.getTotalPointsNum());
  }

  /**
   * Tablet with no failures but some null values in bitmap. Since
   * includeNullValueInWriteThroughputMetric defaults to false, null values are deducted.
   *
   * <p>formula: (dataTypes.length(2) - failedNum(0)) * rows(4) - nullPoints(2) = 6
   */
  @Test
  public void testInsertTablet_noFailure_withNullBits_pointsInsertedNotNegative()
      throws IllegalPathException, WriteProcessException {
    int rowCount = 4;
    // column 1: rows 0 and 2 are null
    BitMap[] bitMaps = new BitMap[2];
    bitMaps[0] = null;
    bitMaps[1] = new BitMap(rowCount);
    bitMaps[1].mark(0);
    bitMaps[1].mark(2);

    InsertTabletNode node =
        buildInsertTabletNode(new String[] {"s0", "s1"}, rowCount, bitMaps, -1 /* no failure */);

    int points = memTable.insertTablet(node, 0, rowCount);

    assertEquals(6, points);
    assertEquals(6, memTable.getTotalPointsNum());
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Builds an aligned InsertRowNode. If {@code failedIndex >= 0} that measurement is marked failed
   * via {@link InsertRowNode#markFailedMeasurement(int)}.
   */
  private static InsertRowNode buildAlignedInsertRowNode(
      String[] measurementNames, Object[] values, int failedIndex) throws IllegalPathException {
    int n = measurementNames.length;
    TSDataType[] dataTypes = new TSDataType[n];
    MeasurementSchema[] schemas = new MeasurementSchema[n];
    for (int i = 0; i < n; i++) {
      dataTypes[i] = TSDataType.INT32;
      schemas[i] = new MeasurementSchema(measurementNames[i], TSDataType.INT32);
    }
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("test"),
            new PartialPath("root.sg.d1"),
            true /* isAligned */,
            measurementNames,
            dataTypes,
            schemas,
            1L,
            values,
            false);
    if (failedIndex >= 0) {
      node.markFailedMeasurement(failedIndex);
      node.setFailedMeasurementNumber(1);
    }
    return node;
  }

  /**
   * Builds a non-aligned InsertTabletNode. If {@code failedIndex >= 0} that measurement is marked
   * failed via {@link InsertTabletNode#markFailedMeasurement(int)}.
   */
  private static InsertTabletNode buildInsertTabletNode(
      String[] measurementNames, int rowCount, BitMap[] bitMaps, int failedIndex)
      throws IllegalPathException {
    int n = measurementNames.length;
    TSDataType[] dataTypes = new TSDataType[n];
    Object[] columns = new Object[n];
    MeasurementSchema[] schemas = new MeasurementSchema[n];
    for (int i = 0; i < n; i++) {
      dataTypes[i] = TSDataType.INT32;
      columns[i] = new int[rowCount];
      schemas[i] = new MeasurementSchema(measurementNames[i], TSDataType.INT32);
    }
    long[] times = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      times[i] = i + 1L;
    }
    InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("test"),
            new PartialPath("root.sg.d1"),
            false /* isAligned */,
            measurementNames,
            dataTypes,
            schemas,
            times,
            bitMaps,
            columns,
            rowCount);
    if (failedIndex >= 0) {
      node.markFailedMeasurement(failedIndex);
      node.setFailedMeasurementNumber(1);
    }
    return node;
  }
}
