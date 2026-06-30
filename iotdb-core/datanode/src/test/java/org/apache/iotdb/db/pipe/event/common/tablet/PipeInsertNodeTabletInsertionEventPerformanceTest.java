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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern.buildUnionPattern;

public class PipeInsertNodeTabletInsertionEventPerformanceTest {

  private static final String MANUAL_OBJECT_FAST_PATH_PERFORMANCE_TEST =
      "iotdb.pipe.insert.node.object.fastpath.performance.enabled";

  private static final int MEASUREMENT_COUNT = 8_000;
  private static final int ROW_COUNT = 2_000;
  private static final int TABLET_COUNT = 1;
  private static final int WARM_UP_ROUNDS = 100;
  private static final int MEASURE_ROUNDS = 1_000;

  @Test
  public void testMayContainObjectData() throws Exception {
    Assert.assertFalse(createInsertTabletNode("non-object", false, 4, 2).mayContainObjectData());
    Assert.assertTrue(createInsertTabletNode("object", true, 4, 2).mayContainObjectData());

    final InsertRowsNode nonObjectRowsNode = new InsertRowsNode(new PlanNodeId("non-object-rows"));
    Assert.assertFalse(nonObjectRowsNode.mayContainObjectData());
    nonObjectRowsNode.addOneInsertRowNode(createInsertRowNode("non-object-row", false), 0);
    Assert.assertFalse(nonObjectRowsNode.mayContainObjectData());
    nonObjectRowsNode.setDataTypes(new TSDataType[] {TSDataType.OBJECT});
    Assert.assertFalse(nonObjectRowsNode.mayContainObjectData());

    final InsertRowsNode objectRowsNode = new InsertRowsNode(new PlanNodeId("object-rows"));
    objectRowsNode.addOneInsertRowNode(createInsertRowNode("object-row", true), 0);
    Assert.assertTrue(objectRowsNode.mayContainObjectData());
    objectRowsNode.setDataTypes(new TSDataType[] {TSDataType.TEXT});
    Assert.assertTrue(objectRowsNode.mayContainObjectData());

    final InsertRowsOfOneDeviceNode nonObjectRowsOfOneDeviceNode =
        new InsertRowsOfOneDeviceNode(new PlanNodeId("non-object-one-device-rows"));
    Assert.assertFalse(nonObjectRowsOfOneDeviceNode.mayContainObjectData());
    nonObjectRowsOfOneDeviceNode.addOneInsertRowNode(
        createInsertRowNode("non-object-one-device-row", false), 0);
    Assert.assertFalse(nonObjectRowsOfOneDeviceNode.mayContainObjectData());
    nonObjectRowsOfOneDeviceNode.setDataTypes(new TSDataType[] {TSDataType.OBJECT});
    Assert.assertFalse(nonObjectRowsOfOneDeviceNode.mayContainObjectData());

    final InsertRowsOfOneDeviceNode objectRowsOfOneDeviceNode =
        new InsertRowsOfOneDeviceNode(new PlanNodeId("object-one-device-rows"));
    objectRowsOfOneDeviceNode.addOneInsertRowNode(
        createInsertRowNode("object-one-device-row", true), 0);
    Assert.assertTrue(objectRowsOfOneDeviceNode.mayContainObjectData());
    objectRowsOfOneDeviceNode.setDataTypes(new TSDataType[] {TSDataType.TEXT});
    Assert.assertTrue(objectRowsOfOneDeviceNode.mayContainObjectData());

    final InsertMultiTabletsNode nonObjectMultiTabletsNode =
        new InsertMultiTabletsNode(new PlanNodeId("non-object-multi-tablets"));
    Assert.assertFalse(nonObjectMultiTabletsNode.mayContainObjectData());
    nonObjectMultiTabletsNode.addInsertTabletNode(
        createInsertTabletNode("non-object-tablet", false, 4, 2), 0);
    Assert.assertFalse(nonObjectMultiTabletsNode.mayContainObjectData());
    nonObjectMultiTabletsNode.setDataTypes(new TSDataType[] {TSDataType.OBJECT});
    Assert.assertFalse(nonObjectMultiTabletsNode.mayContainObjectData());

    final InsertMultiTabletsNode objectMultiTabletsNode =
        new InsertMultiTabletsNode(new PlanNodeId("object-multi-tablets"));
    objectMultiTabletsNode.addInsertTabletNode(
        createInsertTabletNode("object-tablet", true, 4, 2), 0);
    Assert.assertTrue(objectMultiTabletsNode.mayContainObjectData());
    objectMultiTabletsNode.setDataTypes(new TSDataType[] {TSDataType.TEXT});
    Assert.assertTrue(objectMultiTabletsNode.mayContainObjectData());

    final InsertRowNode objectRowNode = createInsertRowNode("failed-object-row", true);
    objectRowNode.markFailedMeasurement(1);
    Assert.assertFalse(objectRowNode.mayContainObjectData());

    final InsertTabletNode objectTabletNode =
        createInsertTabletNode("failed-object-tablet", true, 4, 2);
    objectTabletNode.markFailedMeasurement(3);
    Assert.assertFalse(objectTabletNode.mayContainObjectData());
  }

  @Test
  public void testIncreaseResourceReferenceSkipsObjectPathScanForNonObjectInsertNode()
      throws Exception {
    final PipeInsertNodeTabletInsertionEvent event =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.db",
            createInsertTabletNode("non-object-increase", false, 4, 2),
            "manual_pipe",
            1,
            null,
            buildUnionPattern(false, Collections.singletonList(new IoTDBTreePattern(false, null))),
            new TablePattern(true, null, null),
            "0",
            "user",
            "localhost",
            false,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            null);

    try {
      Assert.assertTrue(event.internallyIncreaseResourceReferenceCount("test"));
      Assert.assertFalse(event.hasObjectData());
    } finally {
      event.internallyDecreaseResourceReferenceCount("test");
    }
  }

  @Test
  public void testManualObjectFastPathPerformance() throws Exception {
    Assume.assumeTrue(
        "Set -D" + MANUAL_OBJECT_FAST_PATH_PERFORMANCE_TEST + "=true to run this manual test.",
        Boolean.getBoolean(MANUAL_OBJECT_FAST_PATH_PERFORMANCE_TEST));

    final InsertNode insertNode =
        createInsertTabletNode("manual-performance", false, MEASUREMENT_COUNT, ROW_COUNT);
    final PipeInsertNodeTabletInsertionEvent event = createEvent(insertNode);

    final long expectedScannedColumns = MEASUREMENT_COUNT;
    Assert.assertFalse(insertNode.mayContainObjectData());
    Assert.assertEquals(expectedScannedColumns, countNonObjectColumns(insertNode));

    for (int i = 0; i < WARM_UP_ROUNDS; i++) {
      Assert.assertEquals(0, scanLegacyObjectPaths(event));
      insertNode.mayContainObjectData();
    }

    final long legacyScanElapsedNanos =
        measureNanos(
            MEASURE_ROUNDS,
            () -> {
              final long objectPathCount = scanLegacyObjectPaths(event);
              if (objectPathCount != 0) {
                throw new AssertionError(objectPathCount);
              }
            });
    final long fastPathElapsedNanos =
        measureNanos(
            MEASURE_ROUNDS,
            () -> {
              if (insertNode.mayContainObjectData()) {
                throw new AssertionError("Non-object insert node should skip object path scan.");
              }
            });

    final double legacyScanAverageMicros = nanosToMicros(legacyScanElapsedNanos, MEASURE_ROUNDS);
    final double fastPathAverageMicros = nanosToMicros(fastPathElapsedNanos, MEASURE_ROUNDS);
    final double speedup = (double) legacyScanElapsedNanos / fastPathElapsedNanos;
    final double reduction =
        100.0D * (legacyScanElapsedNanos - fastPathElapsedNanos) / legacyScanElapsedNanos;

    System.out.printf(
        Locale.ROOT,
        "%nPipe insert node object fast-path performance:%n"
            + "  tablets=%d, measurements/tablet=%d, rows/tablet=%d, "
            + "scanned non-object columns/round=%d%n"
            + "  legacy object path scan avg: %.3f us%n"
            + "  fast path object type check avg: %.3f us%n"
            + "  speedup: %.2fx, elapsed reduction: %.2f%%%n",
        TABLET_COUNT,
        MEASUREMENT_COUNT,
        ROW_COUNT,
        expectedScannedColumns,
        legacyScanAverageMicros,
        fastPathAverageMicros,
        speedup,
        reduction);

    Assert.assertTrue(fastPathElapsedNanos > 0);
  }

  private static long scanLegacyObjectPaths(final PipeInsertNodeTabletInsertionEvent event) {
    final Iterator<String> iterator = event.objectPaths().iterator();
    long objectPathCount = 0;
    while (iterator.hasNext()) {
      iterator.next();
      objectPathCount++;
    }
    return objectPathCount;
  }

  private static long countNonObjectColumns(final InsertNode insertNode) {
    if (insertNode instanceof InsertMultiTabletsNode insertMultiTabletsNode) {
      long count = 0;
      for (final InsertTabletNode tabletNode : insertMultiTabletsNode.getInsertTabletNodeList()) {
        count += countNonObjectColumns(tabletNode);
      }
      return count;
    }
    if (insertNode instanceof InsertTabletNode insertTabletNode) {
      return insertTabletNode.getDataTypes().length;
    }
    if (insertNode instanceof InsertRowsNode insertRowsNode) {
      long count = 0;
      for (final InsertRowNode rowNode : insertRowsNode.getInsertRowNodeList()) {
        count += countNonObjectColumns(rowNode);
      }
      return count;
    }
    return insertNode.getDataTypes().length;
  }

  private static long measureNanos(final int rounds, final Runnable runnable) {
    final long startNanos = System.nanoTime();
    for (int i = 0; i < rounds; i++) {
      runnable.run();
    }
    return System.nanoTime() - startNanos;
  }

  private static double nanosToMicros(final long nanos, final int rounds) {
    return nanos / (double) rounds / TimeUnit.MICROSECONDS.toNanos(1);
  }

  private static PipeInsertNodeTabletInsertionEvent createEvent(final InsertNode insertNode) {
    return new PipeInsertNodeTabletInsertionEvent(
        false,
        "root.db",
        insertNode,
        null,
        0,
        null,
        buildUnionPattern(false, Collections.singletonList(new IoTDBTreePattern(false, null))),
        new TablePattern(true, null, null),
        "0",
        "user",
        "localhost",
        false,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        null);
  }

  private static InsertTabletNode createInsertTabletNode(
      final String planNodeId,
      final boolean hasObjectColumn,
      final int measurementCount,
      final int rowCount)
      throws IllegalPathException {
    final String[] measurements = new String[measurementCount];
    final TSDataType[] dataTypes = new TSDataType[measurementCount];
    final MeasurementSchema[] measurementSchemas = new MeasurementSchema[measurementCount];
    final Object[] columns = new Object[measurementCount];
    for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
      measurements[measurementIndex] = "s" + measurementIndex;
      final boolean isObjectColumn = hasObjectColumn && measurementIndex == measurementCount - 1;
      dataTypes[measurementIndex] = isObjectColumn ? TSDataType.OBJECT : TSDataType.TEXT;
      measurementSchemas[measurementIndex] =
          new MeasurementSchema(measurements[measurementIndex], dataTypes[measurementIndex]);
      columns[measurementIndex] = createBinaryColumn(rowCount, "v" + measurementIndex);
    }

    final long[] times = new long[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      times[rowIndex] = rowIndex;
    }

    return new InsertTabletNode(
        new PlanNodeId(planNodeId),
        new PartialPath("root.db.d" + planNodeId.replace('-', '_')),
        false,
        measurements,
        dataTypes,
        measurementSchemas,
        times,
        null,
        columns,
        rowCount);
  }

  private static InsertRowNode createInsertRowNode(
      final String planNodeId, final boolean hasObjectColumn) throws IllegalPathException {
    final TSDataType[] dataTypes = {
      TSDataType.TEXT, hasObjectColumn ? TSDataType.OBJECT : TSDataType.TEXT
    };
    final String[] measurements = {"s0", "s1"};
    final MeasurementSchema[] measurementSchemas = {
      new MeasurementSchema(measurements[0], dataTypes[0]),
      new MeasurementSchema(measurements[1], dataTypes[1])
    };
    final Object[] values = {
      new Binary("v0", TSFileConfig.STRING_CHARSET), new Binary("v1", TSFileConfig.STRING_CHARSET)
    };
    return new InsertRowNode(
        new PlanNodeId(planNodeId),
        new PartialPath("root.db." + planNodeId.replace('-', '_')),
        false,
        measurements,
        dataTypes,
        measurementSchemas,
        0,
        values,
        false);
  }

  private static Binary[] createBinaryColumn(final int rowCount, final String value) {
    final Binary[] values = new Binary[rowCount];
    final Binary binaryValue = new Binary(value, TSFileConfig.STRING_CHARSET);
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      values[rowIndex] = binaryValue;
    }
    return values;
  }
}
