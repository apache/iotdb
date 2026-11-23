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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.calc.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionMemMetric;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.MTreeBelowSGMemoryImpl;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class DeviceBlackListConstructorTest {

  @Test
  public void testUnfilteredRetryAndRollback() throws Exception {
    checkRetryAndRollback(false);
  }

  @Test
  public void testFilteredRetryAndRollback() throws Exception {
    checkRetryAndRollback(true);
  }

  private void checkRetryAndRollback(final boolean filtered) throws Exception {
    final MemSchemaRegionStatistics statistics =
        new MemSchemaRegionStatistics(0, new MemSchemaEngineStatistics());
    final MTreeBelowSGMemoryImpl tree =
        new MTreeBelowSGMemoryImpl(
            PartialPath.getQualifiedDatabasePartialPath("root.db"),
            node -> Collections.emptyMap(),
            node -> Collections.emptyMap(),
            statistics,
            new SchemaRegionMemMetric(statistics, "root.db"));
    final AtomicInteger pointer = new AtomicInteger();
    tree.createOrUpdateTableDevice(
        "t", new String[] {"d1"}, pointer::getAndIncrement, ignored -> {});
    tree.createOrUpdateTableDevice(
        "t", new String[] {"d2"}, pointer::getAndIncrement, ignored -> {});
    assertEquals(2, statistics.getTableDevicesNumber("t"));

    final PartialPath allDevices = new PartialPath("root.db.t.*");
    // Both an overlapping pattern in one request and a new request after a retry visit d1 again.
    for (int retry = 0; retry < 2; retry++) {
      final DeviceBlackListConstructor constructor = newConstructor(statistics, filtered, true);
      try (final DeviceBlackListConstructor ignored = constructor) {
        tree.constructTableDeviceBlackList(new PartialPath("root.db.t.d1"), constructor);
        tree.constructTableDeviceBlackList(allDevices, constructor);
      }
      assertEquals(3, constructor.getPreDeletedNum());
      assertEquals(0, statistics.getTableDevicesNumber("t"));
    }
    tree.rollbackTableDeviceBlackList(allDevices);
    tree.rollbackTableDeviceBlackList(allDevices);
    assertEquals(2, statistics.getTableDevicesNumber("t"));

    final DeviceBlackListConstructor noMatch = newConstructor(statistics, true, false);
    try (final DeviceBlackListConstructor ignored = noMatch) {
      tree.constructTableDeviceBlackList(allDevices, noMatch);
    }
    assertEquals(0, noMatch.getPreDeletedNum());
    assertEquals(2, statistics.getTableDevicesNumber("t"));

    final DeviceBlackListConstructor delete = newConstructor(statistics, filtered, true);
    try (final DeviceBlackListConstructor ignored = delete) {
      tree.constructTableDeviceBlackList(allDevices, delete);
    }
    final AtomicInteger deleted = new AtomicInteger();
    tree.deleteTableDevicesInBlackList(
        allDevices, ignored -> deleted.incrementAndGet(), ignored -> {});
    assertEquals(2, deleted.get());
    assertEquals(0, statistics.getTableDevicesNumber("t"));
  }

  private DeviceBlackListConstructor newConstructor(
      final MemSchemaRegionStatistics statistics, final boolean filtered, final boolean matches) {
    final ConstantColumnTransformer filter =
        new ConstantColumnTransformer(
            TypeFactory.getType(TSDataType.BOOLEAN),
            new BooleanColumn(1, Optional.empty(), new boolean[] {matches}));
    filter.addReferenceCount();
    return new DeviceBlackListConstructor(
        filtered ? Collections.singletonList(filter) : Collections.emptyList(),
        filtered ? filter : null,
        "t",
        Collections.emptyList(),
        (pointer, name) -> null,
        statistics,
        "db",
        new TsTable("t"));
  }
}
