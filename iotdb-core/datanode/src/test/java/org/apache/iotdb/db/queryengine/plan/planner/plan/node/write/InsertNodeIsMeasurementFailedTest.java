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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link InsertNode#isMeasurementFailed(int)}.
 *
 * <p>The method was added to fix a bug where failed (partial-insert) measurements were incorrectly
 * counted when computing {@code pointsInserted}, which could produce a negative value.
 */
public class InsertNodeIsMeasurementFailedTest {

  // -----------------------------------------------------------------------
  // InsertRowNode
  // -----------------------------------------------------------------------

  @Test
  public void testInsertRowNode_noFailure_allReturnFalse() throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1", "s2"});

    assertFalse("s0 should not be failed", node.isMeasurementFailed(0));
    assertFalse("s1 should not be failed", node.isMeasurementFailed(1));
    assertFalse("s2 should not be failed", node.isMeasurementFailed(2));
  }

  @Test
  public void testInsertRowNode_markFirstFailed_firstReturnsTrue() throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1", "s2"});
    node.markFailedMeasurement(0);

    assertTrue("s0 should be failed after markFailedMeasurement", node.isMeasurementFailed(0));
    assertFalse("s1 should not be failed", node.isMeasurementFailed(1));
    assertFalse("s2 should not be failed", node.isMeasurementFailed(2));
  }

  @Test
  public void testInsertRowNode_markAllFailed_allReturnTrue() throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1"});
    node.markFailedMeasurement(0);
    node.markFailedMeasurement(1);

    assertTrue(node.isMeasurementFailed(0));
    assertTrue(node.isMeasurementFailed(1));
  }

  @Test
  public void testInsertRowNode_markSameTwice_idempotent() throws IllegalPathException {
    // markFailedMeasurement is a no-op when already null; isMeasurementFailed must stay true
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1"});
    node.markFailedMeasurement(0);
    node.markFailedMeasurement(0); // second call should be a no-op

    assertTrue(node.isMeasurementFailed(0));
    assertFalse(node.isMeasurementFailed(1));
  }

  @Test
  public void testInsertRowNode_clearedMeasurementWithRetainedValue_isFailed()
      throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1"});
    node.getMeasurements()[0] = null;

    assertTrue(node.isMeasurementFailed(0));
    assertNull(node.composeTimeValuePair(0));
  }

  @Test
  public void testInsertRowNode_retainedMeasurementWithNullValueDoesNotComposeLastCacheValue()
      throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1"});
    node.getValues()[0] = null;

    assertFalse(node.isMeasurementFailed(0));
    assertNull(node.composeTimeValuePair(0));
  }

  @Test
  public void testInsertRowNode_nullMeasurements_nullSafe() throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0"});
    node.setMeasurements(null);

    assertTrue(node.isMeasurementFailed(0));
    assertFalse(node.hasValidMeasurements());
    assertTrue(node.allMeasurementFailed());
  }

  @Test
  public void testGetRawMeasurementsReusesMeasurementsWhenSchemaNamesMatch()
      throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"s0", "s1"});

    assertSame(node.getMeasurements(), node.getRawMeasurements());
  }

  @Test
  public void testGetRawMeasurementsCopiesOnlyWhenSchemaNameDiffers() throws IllegalPathException {
    InsertRowNode node = buildInsertRowNode(new String[] {"alias", "s1"});
    node.getMeasurementSchemas()[0] = new MeasurementSchema("s0", TSDataType.INT32);

    String[] rawMeasurements = node.getRawMeasurements();

    assertNotSame(node.getMeasurements(), rawMeasurements);
    assertArrayEquals(new String[] {"s0", "s1"}, rawMeasurements);
    assertArrayEquals(new String[] {"alias", "s1"}, node.getMeasurements());
  }

  @Test
  public void testRelationalInsertRowNode_nonFieldColumnsDoNotComposeLastCacheValue()
      throws IllegalPathException {
    RelationalInsertRowNode node = buildRelationalInsertRowNode();

    assertNull(node.composeTimeValuePair(0));
    assertNull(node.composeTimeValuePair(1));
    assertNotNull(node.composeTimeValuePair(2));
  }

  @Test
  public void testRelationalInsertRowNode_deviceIDSkipsMissingTagValue()
      throws IllegalPathException {
    RelationalInsertRowNode node = buildRelationalInsertRowNodeWithTwoTags(new Object[] {"tag0"});

    assertArrayEquals(new Object[] {"table1", "tag0"}, node.getDeviceID().getSegments());
  }

  @Test
  public void testRelationalInsertRowsNode_deviceIDSkipsMissingTagValue()
      throws IllegalPathException {
    RelationalInsertRowNode rowNode =
        buildRelationalInsertRowNodeWithTwoTags(new Object[] {"tag0"});
    RelationalInsertRowsNode rowsNode = new RelationalInsertRowsNode(new PlanNodeId("test"));
    rowsNode.addOneInsertRowNode(rowNode, 0);

    assertArrayEquals(new Object[] {"table1", "tag0"}, rowsNode.getDeviceID(0).getSegments());
  }

  // -----------------------------------------------------------------------
  // InsertTabletNode
  // -----------------------------------------------------------------------

  @Test
  public void testInsertTabletNode_noFailure_allReturnFalse() throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1", "s2"});

    assertFalse(node.isMeasurementFailed(0));
    assertFalse(node.isMeasurementFailed(1));
    assertFalse(node.isMeasurementFailed(2));
  }

  @Test
  public void testInsertTabletNode_markMiddleFailed_onlyMiddleReturnsTrue()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1", "s2"});
    node.markFailedMeasurement(1);

    assertFalse(node.isMeasurementFailed(0));
    assertTrue("s1 should be failed", node.isMeasurementFailed(1));
    assertFalse(node.isMeasurementFailed(2));
  }

  @Test
  public void testInsertTabletNode_markLastFailed_lastReturnsTrue() throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.markFailedMeasurement(1);

    assertFalse(node.isMeasurementFailed(0));
    assertTrue(node.isMeasurementFailed(1));
  }

  @Test
  public void testInsertTabletNode_clearedMeasurementWithRetainedColumn_isFailed()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.getMeasurements()[0] = null;

    assertTrue(node.isMeasurementFailed(0));
    assertNull(node.composeLastTimeValuePair(0));
  }

  @Test
  public void testInsertTabletNode_retainedMeasurementWithNullColumnDoesNotComposeLastCacheValue()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.getColumns()[0] = null;

    assertFalse(node.isMeasurementFailed(0));
    assertNull(node.composeLastTimeValuePair(0));
  }

  @Test
  public void testInsertTabletNode_markFailedMeasurementHandlesShortColumns()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.setColumns(new Object[] {new int[] {1, 2}});

    node.markFailedMeasurement(1);

    assertTrue(node.isMeasurementFailed(1));
    assertNull(node.getDataType(1));
  }

  @Test
  public void testInsertTabletNode_getDataTypeReturnsNullForShortDataTypes()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.setDataTypes(new TSDataType[] {TSDataType.INT32});

    assertNotNull(node.getDataType(0));
    assertNull(node.getDataType(1));
  }

  @Test
  public void testInsertTabletNode_composeLastTimeValuePairHandlesShortBitMaps()
      throws IllegalPathException {
    InsertTabletNode node = buildInsertTabletNode(new String[] {"s0", "s1"});
    node.setBitMaps(new BitMap[0]);

    assertNotNull(node.composeLastTimeValuePair(1));
  }

  @Test
  public void testInsertTabletNode_equalsHandlesShortDataTypes() throws IllegalPathException {
    InsertTabletNode left = buildInsertTabletNode(new String[] {"s0", "s1"});
    InsertTabletNode right = buildInsertTabletNode(new String[] {"s0", "s1"});
    left.setDataTypes(new TSDataType[] {TSDataType.INT32});
    right.setDataTypes(new TSDataType[] {TSDataType.INT32});

    assertFalse(left.equals(right));
  }

  @Test
  public void testRelationalInsertTabletNode_nonFieldColumnsDoNotComposeLastCacheValue()
      throws IllegalPathException {
    RelationalInsertTabletNode node = buildRelationalInsertTabletNode();

    assertNull(node.composeLastTimeValuePair(0));
    assertNull(node.composeLastTimeValuePair(1));
    assertNotNull(node.composeLastTimeValuePair(2));
  }

  @Test
  public void testRelationalInsertTabletNode_deviceIDSkipsMissingTagColumn()
      throws IllegalPathException {
    RelationalInsertTabletNode node = buildRelationalInsertTabletNodeWithTwoTags();

    assertArrayEquals(new Object[] {"table1", "tag0"}, node.getDeviceID(0).getSegments());
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static InsertRowNode buildInsertRowNode(String[] measurementNames)
      throws IllegalPathException {
    int n = measurementNames.length;
    TSDataType[] dataTypes = new TSDataType[n];
    Object[] values = new Object[n];
    MeasurementSchema[] schemas = new MeasurementSchema[n];
    for (int i = 0; i < n; i++) {
      dataTypes[i] = TSDataType.INT32;
      values[i] = i;
      schemas[i] = new MeasurementSchema(measurementNames[i], TSDataType.INT32);
    }
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("test"),
            new PartialPath("root.sg.d1"),
            false,
            measurementNames,
            dataTypes,
            schemas,
            1L,
            values,
            false);
    return node;
  }

  private static RelationalInsertRowNode buildRelationalInsertRowNode()
      throws IllegalPathException {
    String[] measurements = {"tag0", "attr0", "field0"};
    TSDataType[] dataTypes = {TSDataType.STRING, TSDataType.STRING, TSDataType.INT32};
    MeasurementSchema[] schemas = {
      new MeasurementSchema("tag0", TSDataType.STRING),
      new MeasurementSchema("attr0", TSDataType.STRING),
      new MeasurementSchema("field0", TSDataType.INT32)
    };
    Object[] values = {
      new Binary("tag".getBytes(StandardCharsets.UTF_8)),
      new Binary("attr".getBytes(StandardCharsets.UTF_8)),
      1
    };
    return new RelationalInsertRowNode(
        new PlanNodeId("test"),
        new PartialPath("table1", false),
        true,
        measurements,
        dataTypes,
        schemas,
        1L,
        values,
        false,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG, TsTableColumnCategory.ATTRIBUTE, TsTableColumnCategory.FIELD
        });
  }

  private static RelationalInsertRowNode buildRelationalInsertRowNodeWithTwoTags(Object[] values)
      throws IllegalPathException {
    String[] measurements = {"tag0", "tag1", "field0"};
    TSDataType[] dataTypes = {TSDataType.STRING, TSDataType.STRING, TSDataType.INT32};
    MeasurementSchema[] schemas = {
      new MeasurementSchema("tag0", TSDataType.STRING),
      new MeasurementSchema("tag1", TSDataType.STRING),
      new MeasurementSchema("field0", TSDataType.INT32)
    };
    return new RelationalInsertRowNode(
        new PlanNodeId("test"),
        new PartialPath("table1", false),
        true,
        measurements,
        dataTypes,
        schemas,
        1L,
        values,
        false,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG, TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD
        });
  }

  private static InsertTabletNode buildInsertTabletNode(String[] measurementNames)
      throws IllegalPathException {
    int n = measurementNames.length;
    int rowCount = 3;
    TSDataType[] dataTypes = new TSDataType[n];
    Object[] columns = new Object[n];
    MeasurementSchema[] schemas = new MeasurementSchema[n];
    for (int i = 0; i < n; i++) {
      dataTypes[i] = TSDataType.INT32;
      columns[i] = new int[rowCount];
      schemas[i] = new MeasurementSchema(measurementNames[i], TSDataType.INT32);
    }
    long[] times = {1L, 2L, 3L};
    InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("test"),
            new PartialPath("root.sg.d1"),
            false,
            measurementNames,
            dataTypes,
            schemas,
            times,
            null,
            columns,
            rowCount);
    return node;
  }

  private static RelationalInsertTabletNode buildRelationalInsertTabletNode()
      throws IllegalPathException {
    String[] measurements = {"tag0", "attr0", "field0"};
    TSDataType[] dataTypes = {TSDataType.STRING, TSDataType.STRING, TSDataType.INT32};
    MeasurementSchema[] schemas = {
      new MeasurementSchema("tag0", TSDataType.STRING),
      new MeasurementSchema("attr0", TSDataType.STRING),
      new MeasurementSchema("field0", TSDataType.INT32)
    };
    Object[] columns = {
      new Binary[] {new Binary("tag".getBytes(StandardCharsets.UTF_8))},
      new Binary[] {new Binary("attr".getBytes(StandardCharsets.UTF_8))},
      new int[] {1}
    };
    return new RelationalInsertTabletNode(
        new PlanNodeId("test"),
        new PartialPath("table1", false),
        true,
        measurements,
        dataTypes,
        schemas,
        new long[] {1L},
        null,
        columns,
        1,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG, TsTableColumnCategory.ATTRIBUTE, TsTableColumnCategory.FIELD
        });
  }

  private static RelationalInsertTabletNode buildRelationalInsertTabletNodeWithTwoTags()
      throws IllegalPathException {
    String[] measurements = {"tag0", "tag1", "field0"};
    TSDataType[] dataTypes = {TSDataType.STRING, TSDataType.STRING, TSDataType.INT32};
    MeasurementSchema[] schemas = {
      new MeasurementSchema("tag0", TSDataType.STRING),
      new MeasurementSchema("tag1", TSDataType.STRING),
      new MeasurementSchema("field0", TSDataType.INT32)
    };
    Object[] columns = {new Binary[] {new Binary("tag0".getBytes(StandardCharsets.UTF_8))}};
    return new RelationalInsertTabletNode(
        new PlanNodeId("test"),
        new PartialPath("table1", false),
        true,
        measurements,
        dataTypes,
        schemas,
        new long[] {1L},
        null,
        columns,
        1,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG, TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD
        });
  }
}
