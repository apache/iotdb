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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICES_REGION_GROUP;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_ID;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SHANGHAI_SHENZHEN_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SHENZHEN_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SortTest {

  static Metadata metadata = new TestMetadata();
  String sql;
  Analysis analysis;
  MPPQueryContext context;
  WarningCollector warningCollector = WarningCollector.NOOP;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  OutputNode outputNode;
  ProjectNode projectNode;
  StreamSortNode streamSortNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  DeviceTableScanNode deviceTableScanNode;

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  // order by some_ids, time, others; has filter
  @Test
  public void someIDColumnTimeOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(logicalPlanNode, 4);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertEquals(4, deviceTableScanNode.getTagAndAttributeIndexMap().size());
    assertEquals(ASC, deviceTableScanNode.getScanOrder());
    assertEquals(0, deviceTableScanNode.getPushDownLimit());
    assertEquals(0, deviceTableScanNode.getPushDownOffset());
    assertFalse(deviceTableScanNode.isPushLimitToEachDevice());

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to
    // `Output-Offset-Project-TopK-Exchange-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof StreamSortNode);
      assertTrue(getChildrenNode(rootOfFI, 3) instanceof ProjectNode);
      assertTrue(getChildrenNode(rootOfFI, 4) instanceof FilterNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 5);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), DESC, 0, 0, false, "");
    }

    sql = "SELECT * FROM table1 order by tag2 desc, tag3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output-Offset-Limit-StreamSort-TableScan`
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof StreamSortNode);
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    // DistributedPlan: `Output-Offset-TopK-Exchange-Limit-TableScan`
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof TopKNode);
    topKNode = (TopKNode) getChildrenNode(identitySinkNode, 3);

    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 2);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), ASC, 15, 0, true, "");
    }
  }

  // order by all_ids, time, others
  // with limit and offset, use TopKNode
  @Test
  public void allIDColumnTimeSortWithLimitTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof StreamSortNode);
    StreamSortNode streamSortNode = (StreamSortNode) getChildrenNode(logicalPlanNode, 4);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to `Output-Offset-Project-TopK-Exchange-Limit-Project-Filter-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    // childrenDataInOrder==true, and can eliminate the StreamSortNode
    assertTrue(topKNode.isChildrenDataInOrder());

    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof ProjectNode);
      assertTrue(getChildrenNode(rootOfFI, 3) instanceof FilterNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 4);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), DESC, 0, 0, false, "");
    }
  }

  // order by all_ids, time, others
  // without limit and offset, use MergeSortNode
  @Test
  public void allIDColumnTimeSortNoLimitTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, time desc, s1+s2 desc";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof StreamSortNode);
    StreamSortNode streamSortNode = (StreamSortNode) getChildrenNode(logicalPlanNode, 2);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());

    // DistributePlan: optimize `Output-Project-MergeSort-Exchange-Project-Filter-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 2);

    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof ProjectNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof FilterNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 3);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), DESC, 0, 0, false, "");
    }
  }

  // order by some_ids, others, time
  @Test
  public void someIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(logicalPlanNode, 4);
    assertEquals(1, streamSortNode.getStreamCompareKeyEndIndex());
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertEquals(4, deviceTableScanNode.getTagAndAttributeIndexMap().size());
    assertEquals(ASC, deviceTableScanNode.getScanOrder());
    assertEquals(0, deviceTableScanNode.getPushDownLimit());
    assertEquals(0, deviceTableScanNode.getPushDownOffset());
    assertFalse(deviceTableScanNode.isPushLimitToEachDevice());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan` to
    // `Output-Offset-Project-TopK-Exchange-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof StreamSortNode);
      assertTrue(getChildrenNode(rootOfFI, 3) instanceof ProjectNode);
      assertTrue(getChildrenNode(rootOfFI, 4) instanceof FilterNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 5);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), ASC, 0, 0, false, "");
    }
  }

  // order by all_ids, others, time
  @Test
  public void allIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(logicalPlanNode, 4);
    assertEquals(2, streamSortNode.getStreamCompareKeyEndIndex());
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertEquals(4, deviceTableScanNode.getTagAndAttributeIndexMap().size());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to `Output-Offset-Project-TopK-Exchange-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof StreamSortNode);
      assertTrue(getChildrenNode(rootOfFI, 3) instanceof ProjectNode);
      assertTrue(getChildrenNode(rootOfFI, 4) instanceof FilterNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 5);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), ASC, 0, 0, false, "");
    }
  }

  @Test
  public void orderByTimeTest() {
    // order by time, some_ids, others; no filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 order by time desc, tag2 asc, tag3 desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    assertTopKNoFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        DESC,
        15,
        0,
        true,
        symbolAllocator);

    // order by time, others, some_ids; has filter
    sql =
        "SELECT time, tag3, substring(tag1, 1), cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        DESC,
        0,
        0,
        false,
        symbolAllocator);

    // order by time, others, all_ids; has filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag3 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        DESC,
        0,
        0,
        false,
        symbolAllocator);

    // order by time, all_ids, others; has filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, tag2 asc, tag3 desc, tag1 asc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        DESC,
        0,
        0,
        false,
        symbolAllocator);
  }

  @Test
  public void orderByOthersTest() {
    // order by others, some_ids, time
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "order by s1+s2 desc, tag2 desc, tag1 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKNoFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        ASC,
        0,
        0,
        false,
        symbolAllocator);

    // order by others, all_ids, time
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, tag2 desc, tag1 desc, tag3 desc, time asc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        ASC,
        0,
        0,
        false,
        symbolAllocator);

    // order by others, time, some_ids
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        ASC,
        0,
        0,
        false,
        symbolAllocator);

    // order by others, time, all_ids
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc, tag3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(
        SHANGHAI_SHENZHEN_DEVICE_ENTRIES,
        SHENZHEN_DEVICE_ENTRIES,
        ASC,
        0,
        0,
        false,
        symbolAllocator);
  }

  @Test
  public void projectSortTest() {
    // columns in order and select is different
    sql = "SELECT time, attr1, s1 FROM table1 order by attr2 limit 5";
    context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(context, metadata, SESSION_INFO, symbolAllocator, warningCollector)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    IdentitySinkNode sinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(sinkNode, 1) instanceof OutputNode);
    assertTrue(getChildrenNode(sinkNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(sinkNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(sinkNode, 3);
    assertEquals(4, topKNode.getOutputSymbols().size());
  }

  public void assertTopKWithFilter(
      List<String> deviceEntries1,
      List<String> deviceEntries2,
      Ordering expectedOrdering,
      long expectedPushDownLimit,
      long expectedPushDownOffset,
      boolean isPushLimitToEachDevice,
      SymbolAllocator symbolAllocator) {
    // LogicalPlan: `Output - Offset - Project - TopK - Project - FilterNode - TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 5) instanceof FilterNode);
    assertTrue(getChildrenNode(logicalPlanNode, 6) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(logicalPlanNode, 6);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertTrue(
        deviceTableScanNode.getPushDownLimit() == expectedPushDownLimit
            && deviceTableScanNode.getPushDownOffset() == expectedPushDownOffset);
    assertEquals(isPushLimitToEachDevice, deviceTableScanNode.isPushLimitToEachDevice());

    // DistributePlan `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof OutputNode);
    OutputNode outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(2).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof FilterNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 4);
    assertEquals(4, deviceTableScanNode.getDeviceEntries().size());
    assertTableScan(
        deviceTableScanNode,
        deviceEntries1,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice,
        "");

    // IdentitySink - TopK - Project - Filter - TableScan
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof FilterNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 4);
    assertEquals(2, deviceTableScanNode.getDeviceEntries().size());

    assertTableScan(
        deviceTableScanNode,
        deviceEntries2,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice,
        "");
  }

  public void assertTopKNoFilter(
      List<String> deviceEntries1,
      List<String> deviceEntries2,
      Ordering expectedOrdering,
      long expectedPushDownLimit,
      long expectedPushDownOffset,
      boolean isPushLimitToEachDevice,
      SymbolAllocator symbolAllocator) {
    // LogicalPlan: `Output - Offset - Project - TopK - Project -  TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 5) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(logicalPlanNode, 5);
    assertEquals("testdb.table1", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertEquals(4, deviceTableScanNode.getTagAndAttributeIndexMap().size());
    assertEquals(expectedPushDownLimit, deviceTableScanNode.getPushDownLimit());
    assertEquals(expectedPushDownOffset, deviceTableScanNode.getPushDownOffset());
    assertEquals(isPushLimitToEachDevice, deviceTableScanNode.isPushLimitToEachDevice());

    // DistributePlan `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof OutputNode);
    OutputNode outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(2).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 3);
    assertEquals(4, deviceTableScanNode.getDeviceEntries().size());
    assertTableScan(
        deviceTableScanNode,
        deviceEntries1,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice,
        "");

    // IdentitySink - TopK - Project - TableScan
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 3);
    assertEquals(2, deviceTableScanNode.getDeviceEntries().size());
    assertTableScan(
        deviceTableScanNode,
        deviceEntries2,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice);
  }
}
