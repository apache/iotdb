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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertNumericRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.utils.NumericRowValues;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AbstractMemTableNumericRowTest {
  private PrimitiveMemTable memTable;
  private boolean previousIncludeNull;

  @Before
  public void setUp() {
    memTable = new PrimitiveMemTable("numeric_test", "0");
    previousIncludeNull =
        IoTDBDescriptor.getInstance().getConfig().isIncludeNullValueInWriteThroughputMetric();
    IoTDBDescriptor.getInstance().getConfig().setIncludeNullValueInWriteThroughputMetric(false);
  }

  @After
  public void tearDown() {
    memTable.release();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setIncludeNullValueInWriteThroughputMetric(previousIncludeNull);
  }

  @Test
  public void testNullAndFailedFieldAccountingWithBothMetricSettings() throws Exception {
    // TAGs and failed FIELDs never count; valid NULL FIELDs count only in the enabled metric.
    NumericRowValues values = new NumericRowValues(1, 3);
    values.setTag(0, "device");
    values.setRawValue(1, Double.doubleToRawLongBits(10));
    values.setNull(2);
    RelationalInsertNumericRowNode row =
        numericRow(new String[] {"tag", "a", "b", "failed"}, 1, values, 1);
    row.markFailedMeasurement(3);

    assertEquals(1, memTable.insertAlignedRow(row));
    assertEquals(2, memTable.getTotalValueCount());
    assertEquals(1, memTable.getNullValueCount());
    assertEquals(1, memTable.getSeriesNumber());
    assertEquals(20, memTable.memSize());

    IoTDBDescriptor.getInstance().getConfig().setIncludeNullValueInWriteThroughputMetric(true);
    row.setTime(2);
    assertEquals(2, memTable.insertAlignedRow(row));
    values.setNull(1);
    row.setTime(3);
    assertEquals(2, memTable.insertAlignedRow(row));
    assertEquals(5, memTable.getTotalPointsNum());
    assertEquals(6, memTable.getTotalValueCount());
    assertEquals(4, memTable.getNullValueCount());
    assertEquals(52, memTable.memSize());
    assertEquals(1, memTable.getSeriesNumber());
    AlignedTVList list = chunk(row).getWorkingTVList();
    assertEquals(10, list.getDoubleByValueIndex(0, 0), 0);
    assertTrue(list.isNullValue(2, 0));
  }

  @Test
  public void testMissingSlotsAndInvalidMetadataAreNotCounted() throws Exception {
    // A valid schema beyond the value buffer is absent, not a NULL point or a new series.
    NumericRowValues values = new NumericRowValues(1, 1);
    values.setTag(0, "device");
    values.setRawValue(1, Double.doubleToRawLongBits(12));
    RelationalInsertNumericRowNode row =
        numericRow(new String[] {"tag", "a", "missing", "invalid"}, 1, values, 1);
    row.getMeasurementSchemas()[3] = null;
    assertEquals(1, memTable.insertAlignedRow(row));
    assertEquals(1, memTable.getTotalValueCount());
    assertEquals(0, memTable.getNullValueCount());
    assertEquals(1, memTable.getSeriesNumber());
    assertEquals(20, memTable.memSize());
    assertEquals(12, chunk(row).getWorkingTVList().getDoubleByValueIndex(0, 0), 0);
  }

  @Test
  public void testExistingGroupCanGrowAfterNumericAndTabletWrites() throws Exception {
    // Creation filters NULL schemas; a later Tablet materializes that field in the same group.
    NumericRowValues values = new NumericRowValues(0, 2);
    values.setRawValue(0, Double.doubleToRawLongBits(1));
    values.setNull(1);
    RelationalInsertNumericRowNode row = numericRow(new String[] {"a", "b"}, 0, values, 1);
    assertEquals(1, memTable.insertAlignedRow(row));
    AlignedWritableMemChunk original = chunk(row);
    row.setTime(2);
    assertEquals(1, memTable.insertAlignedRow(row));
    RelationalInsertTabletNode tablet =
        new RelationalInsertTabletNode(
            new PlanNodeId("tablet"),
            new PartialPath("t"),
            true,
            new String[] {"b"},
            new TSDataType[] {TSDataType.DOUBLE},
            new MeasurementSchema[] {new MeasurementSchema("b", TSDataType.DOUBLE)},
            new long[] {3},
            null,
            new Object[] {new double[] {30}},
            1,
            new TsTableColumnCategory[] {TsTableColumnCategory.FIELD});
    assertEquals(1, memTable.insertAlignedTablet(tablet, 0, 1, null));
    values.setRawValue(0, Double.doubleToRawLongBits(4));
    values.setRawValue(1, Double.doubleToRawLongBits(40));
    row.setTime(4);
    assertEquals(2, memTable.insertAlignedRow(row));
    assertSame(original, chunk(row));
    assertEquals(2, memTable.getSeriesNumber());
    assertEquals(5, memTable.getTotalPointsNum());
    assertEquals(7, memTable.getTotalValueCount());
    assertEquals(2, memTable.getNullValueCount());
    AlignedTVList list = original.getWorkingTVList();
    assertTrue(list.isNullValue(0, 1));
    assertTrue(list.isNullValue(2, 0));
    assertEquals(30, list.getDoubleByValueIndex(2, 1), 0);
    assertEquals(4, list.getDoubleByValueIndex(3, 0), 0);
    assertEquals(40, list.getDoubleByValueIndex(3, 1), 0);
  }

  @Test
  public void testEmptyRowDoesNotCreateGroup() throws Exception {
    // Empty input leaves counters and the device map untouched.
    assertEquals(
        0, memTable.insertAlignedRow(numericRow(new String[0], 0, new NumericRowValues(0, 0), 1)));
    assertTrue(memTable.getMemTableMap().isEmpty());
    assertEquals(0, memTable.memSize());
    assertEquals(0, memTable.getTotalValueCount());
  }

  private AlignedWritableMemChunk chunk(RelationalInsertNumericRowNode row) {
    return ((AlignedWritableMemChunkGroup) memTable.getMemTableMap().get(row.getDeviceID()))
        .getAlignedMemChunk();
  }

  private static RelationalInsertNumericRowNode numericRow(
      String[] names, int tags, NumericRowValues values, long time) throws IllegalPathException {
    TSDataType[] types = new TSDataType[names.length];
    TsTableColumnCategory[] categories = new TsTableColumnCategory[names.length];
    Arrays.fill(types, TSDataType.DOUBLE);
    Arrays.fill(categories, TsTableColumnCategory.FIELD);
    MeasurementSchema[] schemas = new MeasurementSchema[names.length];
    for (int i = 0; i < names.length; i++) {
      if (i < tags) {
        types[i] = TSDataType.STRING;
        categories[i] = TsTableColumnCategory.TAG;
      }
      schemas[i] = new MeasurementSchema(names[i], types[i]);
    }
    return new RelationalInsertNumericRowNode(
        new PlanNodeId("numeric"),
        new PartialPath("t"),
        names,
        types,
        schemas,
        time,
        values,
        categories);
  }
}
