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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnMetadata;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.read.common.type.StringType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RateOrderedInputTest {

  private static final Symbol TAG1 = new Symbol("tag1");
  private static final Symbol TAG2 = new Symbol("tag2");
  private static final Symbol S1 = new Symbol("s1");
  private static final Symbol S2 = new Symbol("s2");
  private static final Symbol TIME_SYMBOL = new Symbol("time");

  @Test
  public void testSingleDeviceIsOrderedPerGroup() {
    assertTrue(isOrderedPerGroup(SINGLE, Collections.emptyList(), scan(1)));
  }

  @Test
  public void testGroupedByAllTagsIsOrderedPerGroup() {
    assertTrue(isOrderedPerGroup(SINGLE, Arrays.asList(TAG1, TAG2), scan(2)));
  }

  @Test
  public void testGroupedBySubsetOfTagsIsNotOrderedPerGroup() {
    assertFalse(isOrderedPerGroup(SINGLE, Collections.singletonList(TAG1), scan(2)));
  }

  @Test
  public void testDescendingScanIsNotOrderedPerGroup() {
    DeviceTableScanNode scan = scan(1);
    scan.setScanOrder(Ordering.DESC);
    assertFalse(isOrderedPerGroup(SINGLE, Collections.emptyList(), scan));
  }

  @Test
  public void testNonSingleStepIsNotOrderedPerGroup() {
    assertFalse(isOrderedPerGroup(PARTIAL, Collections.emptyList(), scan(1)));
  }

  @Test
  public void testGroupingKeysAreResolvedThroughProjections() {
    DeviceTableScanNode scan = scan(2);
    Symbol tag1Out = new Symbol("tag1_out");
    Symbol tag2Out = new Symbol("tag2_out");
    ProjectNode project =
        new ProjectNode(
            new PlanNodeId("project"),
            scan,
            Assignments.builder()
                .put(tag1Out, new SymbolReference("tag1"))
                .put(tag2Out, new SymbolReference("tag2"))
                .build());
    assertTrue(isOrderedPerGroup(SINGLE, Arrays.asList(tag1Out, tag2Out), project));
  }

  @Test
  public void testProjectAndFilterPreserveScan() {
    DeviceTableScanNode scan = scan(1);
    ProjectNode project =
        new ProjectNode(
            new PlanNodeId("project"),
            scan,
            Assignments.builder().put(S1, new SymbolReference("s1")).build());
    assertSame(scan, TableDistributedPlanGenerator.findOrderedDeviceScan(project));

    FilterNode filter =
        new FilterNode(new PlanNodeId("filter"), scan, Mockito.mock(Expression.class));
    assertSame(scan, TableDistributedPlanGenerator.findOrderedDeviceScan(filter));
  }

  @Test
  public void testSortChildIsRejected() {
    DeviceTableScanNode scan = scan(1);
    SortNode sort =
        new SortNode(
            new PlanNodeId("sort"),
            scan,
            new OrderingScheme(
                Collections.singletonList(TIME_SYMBOL),
                Collections.singletonMap(TIME_SYMBOL, ASC_NULLS_LAST)),
            false,
            false);
    assertNull(TableDistributedPlanGenerator.findOrderedDeviceScan(sort));
    assertFalse(isOrderedPerGroup(SINGLE, Collections.emptyList(), sort));
  }

  @Test
  public void testLimitChildIsRejected() {
    DeviceTableScanNode scan = scan(1);
    LimitNode limit = new LimitNode(new PlanNodeId("limit"), scan, 10, Optional.empty());
    assertNull(TableDistributedPlanGenerator.findOrderedDeviceScan(limit));
  }

  @Test
  public void testSingleChildCollectIsRejected() {
    DeviceTableScanNode scan = scan(1);
    CollectNode collect =
        new CollectNode(
            new PlanNodeId("collect"),
            Collections.singletonList(scan),
            Collections.singletonList(S1));
    assertNull(TableDistributedPlanGenerator.findOrderedDeviceScan(collect));
    assertFalse(isOrderedPerGroup(SINGLE, Collections.emptyList(), collect));
  }

  @Test
  public void testUnsupportedScanTypesAreRejected() {
    assertNull(
        TableDistributedPlanGenerator.findOrderedDeviceScan(
            Mockito.mock(AggregationTableScanNode.class)));
    assertNull(
        TableDistributedPlanGenerator.findOrderedDeviceScan(
            Mockito.mock(TreeDeviceViewScanNode.class)));
    assertNull(
        TableDistributedPlanGenerator.findOrderedDeviceScan(
            Mockito.mock(ExternalTsFileScanNode.class)));
  }

  @Test
  public void testTimeArgumentResolvesToNativeTime() {
    DeviceTableScanNode scan = scan(1);
    assertTrue(
        TableDistributedPlanGenerator.isTimeArgumentNativeTime(
            aggregation("irate", S1, TIME_SYMBOL), scan, scan));
  }

  @Test
  public void testTimeArgumentResolvesThroughAliasToNativeTime() {
    DeviceTableScanNode scan = scan(1);
    Symbol value = new Symbol("v");
    Symbol time = new Symbol("t");
    ProjectNode project =
        new ProjectNode(
            new PlanNodeId("project"),
            scan,
            Assignments.builder()
                .put(value, new SymbolReference("s1"))
                .put(time, new SymbolReference("time"))
                .build());
    assertTrue(
        TableDistributedPlanGenerator.isTimeArgumentNativeTime(
            aggregation("irate", value, time), project, scan));
  }

  @Test
  public void testNonTimeColumnTimeArgumentIsRejected() {
    DeviceTableScanNode scan = scan(1);
    assertFalse(
        TableDistributedPlanGenerator.isTimeArgumentNativeTime(
            aggregation("irate", S1, S2), scan, scan));
  }

  private static AggregationNode.Aggregation aggregation(
      String functionName, Symbol value, Symbol time) {
    ResolvedFunction function = Mockito.mock(ResolvedFunction.class);
    BoundSignature signature = Mockito.mock(BoundSignature.class);
    Mockito.when(function.getSignature()).thenReturn(signature);
    Mockito.when(signature.getName()).thenReturn(functionName);
    return new AggregationNode.Aggregation(
        function,
        Arrays.asList(value.toSymbolReference(), time.toSymbolReference()),
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static boolean isOrderedPerGroup(
      AggregationNode.Step step, List<Symbol> groupingKeys, PlanNode child) {
    DeviceTableScanNode scan = TableDistributedPlanGenerator.findOrderedDeviceScan(child);
    if (scan == null) {
      return false;
    }
    return TableDistributedPlanGenerator.isDeviceScanTimeOrderedPerGroup(
        step, groupingKeys, child, scan, fullTableColumnSchema());
  }

  private static DeviceTableScanNode scan(int deviceCount) {
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(TAG1, columnSchema("tag1", TAG));
    assignments.put(TAG2, columnSchema("tag2", TAG));
    assignments.put(S1, columnSchema("s1", FIELD));
    assignments.put(S2, columnSchema("s2", FIELD));
    assignments.put(TIME_SYMBOL, columnSchema("time", TIME));
    Map<Symbol, Integer> tagAndAttributeIndexMap = new HashMap<>();
    tagAndAttributeIndexMap.put(TAG1, 0);
    tagAndAttributeIndexMap.put(TAG2, 1);
    DeviceTableScanNode scan =
        new DeviceTableScanNode(
            new PlanNodeId("scan"),
            new QualifiedObjectName("testdb", "t"),
            new ArrayList<>(assignments.keySet()),
            assignments,
            tagAndAttributeIndexMap);
    List<DeviceEntry> deviceEntries = new ArrayList<>(deviceCount);
    for (int i = 0; i < deviceCount; i++) {
      deviceEntries.add(Mockito.mock(DeviceEntry.class));
    }
    scan.setDeviceEntries(deviceEntries);
    return scan;
  }

  private static Map<Symbol, ColumnSchema> fullTableColumnSchema() {
    Map<Symbol, ColumnSchema> tableColumnSchema = new LinkedHashMap<>();
    tableColumnSchema.put(TAG1, columnSchema("tag1", TAG));
    tableColumnSchema.put(TAG2, columnSchema("tag2", TAG));
    tableColumnSchema.put(S1, columnSchema("s1", FIELD));
    tableColumnSchema.put(S2, columnSchema("s2", FIELD));
    return tableColumnSchema;
  }

  private static ColumnSchema columnSchema(String name, TsTableColumnCategory category) {
    return ColumnSchema.builder(new ColumnMetadata(name, StringType.STRING))
        .setColumnCategory(category)
        .build();
  }
}
