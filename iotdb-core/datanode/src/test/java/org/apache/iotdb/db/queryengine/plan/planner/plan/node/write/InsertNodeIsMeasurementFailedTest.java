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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
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
}
