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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.function.Repeat;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableFunctionSortTest {

  private static final String[] ROW_FUNCTIONS = {
    "TUMBLE(DATA => table1, SIZE => 10m)",
    "HOP(DATA => table1, SLIDE => 5m, SIZE => 10m)",
    "CUMULATE(DATA => table1, STEP => 6m, SIZE => 12m)"
  };

  private static final String CAPACITY =
      "CAPACITY(DATA => table1 PARTITION BY (tag1, tag2, tag3) ORDER BY time, SIZE => 3, SLIDE => 1)";

  @Test
  public void testRowSemanticsEliminatesSortAndOrdersEveryRegionScan() {
    for (String function : ROW_FUNCTIONS) {
      for (boolean descending : new boolean[] {false, true}) {
        String direction = descending ? " DESC NULLS FIRST" : " ASC NULLS LAST";
        PlanTester tester = new PlanTester();
        tester.createPlan(
            "SELECT * FROM "
                + function
                + " ORDER BY tag1"
                + direction
                + ", tag2"
                + direction
                + ", tag3"
                + direction
                + ", time"
                + direction);

        assertTrue(findNodes(tester, SortNode.class).isEmpty());
        assertFalse(findNodes(tester, MergeSortNode.class).isEmpty());
        List<DeviceTableScanNode> scans = findNodes(tester, DeviceTableScanNode.class);
        assertEquals(3, scans.size());
        for (DeviceTableScanNode scan : scans) {
          List<String> actual =
              scan.getDeviceEntries().stream()
                  .map(entry -> entry.getDeviceID().toString())
                  .collect(Collectors.toList());
          List<String> expected = new ArrayList<>(actual);
          expected.sort(descending ? Collections.reverseOrder() : String::compareTo);
          // Merely removing Sort is insufficient: each MergeSort input must actually be sorted.
          assertEquals(expected, actual);
          assertEquals(descending ? DESC : ASC, scan.getScanOrder());
        }
      }
    }
  }

  @Test
  public void testGeneratedColumnAfterDeviceAndTimeKeepsStreamSort() {
    for (String function : ROW_FUNCTIONS) {
      PlanTester tester = new PlanTester();
      tester.createPlan(
          "SELECT * FROM "
              + function
              + " ORDER BY tag1, tag2, tag3, time, window_start DESC, window_end DESC");
      List<StreamSortNode> sorts = findNodes(tester, StreamSortNode.class);
      assertEquals(3, sorts.size());
      for (StreamSortNode sort : sorts) {
        assertFalse(sort.isOrderByAllIdsAndTime());
        assertEquals(2, sort.getStreamCompareKeyEndIndex());
      }
    }
  }

  @Test
  public void testGeneratedLeadingColumnKeepsFullSort() {
    PlanTester tester = new PlanTester();
    tester.createPlan("SELECT * FROM " + ROW_FUNCTIONS[1] + " ORDER BY window_start, tag1, time");
    assertEquals(3, findNodes(tester, SortNode.class).size());
    assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
  }

  @Test
  public void testSetSemanticsDoesNotPreserveOrderWithinPartition() {
    PlanTester tester = new PlanTester();
    tester.createPlan(
        "SELECT * FROM REPEAT(DATA => table1 PARTITION BY (tag1, tag2, tag3) ORDER BY time, N => 2) ORDER BY tag1, tag2, tag3, time");
    List<SortNode> sorts = findNodes(tester, SortNode.class);
    assertFalse(sorts.isEmpty());
    assertTrue(sorts.stream().allMatch(StreamSortNode.class::isInstance));
    for (SortNode sort : sorts) {
      assertEquals(2, ((StreamSortNode) sort).getStreamCompareKeyEndIndex());
      assertFalse(sort.isOrderByAllIdsAndTime());
      assertFalse(findNodes(sort, TableFunctionProcessorNode.class).isEmpty());
    }
  }

  @Test
  public void testSetSemanticsPreservesSortedPartitionKeys() {
    PlanTester tester = new PlanTester();
    tester.createPlan("SELECT * FROM " + CAPACITY + " ORDER BY tag1, tag2, tag3");
    assertFalse(findNodes(tester, TableFunctionProcessorNode.class).isEmpty());
    assertFalse(findNodes(tester, MergeSortNode.class).isEmpty());
    assertFalse(
        findNodes(tester, SortNode.class).stream()
            .anyMatch(sort -> !findNodes(sort, TableFunctionProcessorNode.class).isEmpty()));
  }

  @Test
  public void testSetSemanticsCanOrderPartitionsWithoutSortingRows() {
    PlanTester tester = new PlanTester();
    tester.createPlan("SELECT * FROM " + CAPACITY + " ORDER BY tag1 DESC, tag2, tag3");
    assertTrue(findNodes(tester, SortNode.class).isEmpty());
    Comparator<String> comparator =
        Comparator.comparing((String id) -> id.split("\\.")[1], Comparator.reverseOrder())
            .thenComparing(id -> id.split("\\.")[2])
            .thenComparing(id -> id.split("\\.")[3]);
    for (DeviceTableScanNode scan : findNodes(tester, DeviceTableScanNode.class)) {
      List<String> actual =
          scan.getDeviceEntries().stream()
              .map(entry -> entry.getDeviceID().toString())
              .collect(Collectors.toList());
      List<String> expected = new ArrayList<>(actual);
      expected.sort(comparator);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testLeafFunctionKeepsSort() {
    PlanTester tester = new PlanTester();
    tester.createPlan("SELECT * FROM SPLIT(INPUT => 'z,a') ORDER BY output");
    assertEquals(1, findNodes(tester, SortNode.class).size());
    assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
  }

  @Test
  public void testNestedRowSemanticsPreservesPassThroughOrdering() {
    PlanTester tester = new PlanTester();
    tester.createPlan(
        "SELECT * FROM TUMBLE(DATA => (SELECT time, tag1, tag2, tag3 FROM "
            + ROW_FUNCTIONS[1]
            + "), SIZE => 10m) ORDER BY tag1, tag2, tag3, time DESC");
    assertTrue(findNodes(tester, SortNode.class).isEmpty());
    assertEquals(6, findNodes(tester, TableFunctionProcessorNode.class).size());
    for (DeviceTableScanNode scan : findNodes(tester, DeviceTableScanNode.class)) {
      assertEquals(DESC, scan.getScanOrder());
    }
  }

  @Test
  public void testRowSemanticsWithoutPassThroughKeepsSort() {
    PlanTester tester = new PlanTester();
    tester.createPlan(
        "SELECT * FROM EXCLUDE(DATA => table1, EXCLUDE => 'attr1') ORDER BY tag1, tag2, tag3, time");
    assertEquals(3, findNodes(tester, SortNode.class).size());
    assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
  }

  @Test
  public void testCustomRowSemanticsDoesNotGuaranteeOutputOrder() {
    PlanTester tester =
        new PlanTester(
            new TestMetadata() {
              @Override
              public TableFunction getTableFunction(String name) {
                if (!"repeat".equalsIgnoreCase(name)) {
                  return super.getTableFunction(name);
                }
                // Repeat buffers extra copies until finish(). It then emits earlier input indices
                // again, so declaring row semantics alone cannot establish output ordering.
                return new Repeat() {
                  @Override
                  public List<ParameterSpecification> getArgumentsSpecifications() {
                    List<ParameterSpecification> parameters =
                        new ArrayList<>(super.getArgumentsSpecifications());
                    parameters.set(
                        0,
                        TableParameterSpecification.builder()
                            .name("DATA")
                            .rowSemantics()
                            .passThroughColumns()
                            .build());
                    return parameters;
                  }
                };
              }
            });
    tester.createPlan(
        "SELECT * FROM REPEAT(DATA => table1, N => 2) ORDER BY tag1, tag2, tag3, time");
    assertEquals(3, findNodes(tester, SortNode.class).size());
    assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
    for (TableFunctionProcessorNode function :
        findNodes(tester, TableFunctionProcessorNode.class)) {
      assertTrue(function.isRowSemantic());
      assertTrue(function.getPassThroughSpecification().isPresent());
    }
  }

  @Test
  public void testStreamSortRequiresProvenInputPrefix() {
    PlanTester tester = new PlanTester();
    tester.createPlan(
        "SELECT * FROM TUMBLE(DATA => (SELECT *, diff(s1) AS delta FROM table1), SIZE => 10m)"
            + " ORDER BY tag1, tag2, tag3, delta");
    assertFalse(findNodes(tester, SortNode.class).isEmpty());
    assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
  }

  private static <T extends PlanNode> List<T> findNodes(PlanTester tester, Class<T> type) {
    return tester.getDistributedPlan().getFragments().stream()
        .flatMap(fragment -> findNodes(fragment.getPlanNodeTree(), type).stream())
        .collect(Collectors.toList());
  }

  private static <T extends PlanNode> List<T> findNodes(PlanNode root, Class<T> type) {
    List<T> result = new ArrayList<>();
    if (type.isInstance(root)) {
      result.add(type.cast(root));
    }
    root.getChildren().forEach(child -> result.addAll(findNodes(child, type)));
    return result;
  }
}
