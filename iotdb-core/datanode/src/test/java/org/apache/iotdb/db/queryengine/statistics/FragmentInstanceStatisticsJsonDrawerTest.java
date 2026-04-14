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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FragmentInstanceStatisticsJsonDrawerTest {

  @Test
  public void testRenderPlanStatistics() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    context.setAnalyzeCost(1000000L); // 1ms
    context.setFetchPartitionCost(2000000L); // 2ms
    context.setFetchSchemaCost(3000000L); // 3ms
    context.setLogicalPlanCost(4000000L); // 4ms
    context.setLogicalOptimizationCost(5000000L); // 5ms
    context.setDistributionPlanCost(6000000L); // 6ms

    drawer.renderPlanStatistics(context);

    // Now render to JSON and verify planStatistics
    drawer.renderDispatchCost(context);
    String json =
        drawer.renderFragmentInstancesAsJson(
            Collections.emptyList(), Collections.emptyMap(), false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    JsonObject planStats = root.getAsJsonObject("planStatistics");

    assertNotNull(planStats);
    assertEquals(1.0, planStats.get("analyzeCostMs").getAsDouble(), 0.01);
    assertEquals(2.0, planStats.get("fetchPartitionCostMs").getAsDouble(), 0.01);
    assertEquals(3.0, planStats.get("fetchSchemaCostMs").getAsDouble(), 0.01);
    assertEquals(4.0, planStats.get("logicalPlanCostMs").getAsDouble(), 0.01);
    assertEquals(5.0, planStats.get("logicalOptimizationCostMs").getAsDouble(), 0.01);
    assertEquals(6.0, planStats.get("distributionPlanCostMs").getAsDouble(), 0.01);
  }

  @Test
  public void testRenderDispatchCost() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    context.recordDispatchCost(7000000L); // 7ms

    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    String json =
        drawer.renderFragmentInstancesAsJson(
            Collections.emptyList(), Collections.emptyMap(), false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    JsonObject planStats = root.getAsJsonObject("planStatistics");

    assertNotNull(planStats);
    assertEquals(7.0, planStats.get("dispatchCostMs").getAsDouble(), 0.01);
  }

  @Test
  public void testRenderEmptyFragmentInstances() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    String json =
        drawer.renderFragmentInstancesAsJson(
            Collections.emptyList(), Collections.emptyMap(), false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertNotNull(root);
    assertEquals(0, root.get("fragmentInstancesCount").getAsInt());
    assertTrue(root.has("fragmentInstances"));
    assertEquals(0, root.getAsJsonArray("fragmentInstances").size());
  }

  @Test
  public void testRenderFragmentInstanceWithStatistics() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    // Create a simple plan node tree
    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    // Create statistics
    TFetchFragmentInstanceStatisticsResp stats = new TFetchFragmentInstanceStatisticsResp();
    stats.setDataRegion("root.db.region1");
    stats.setIp("127.0.0.1");
    stats.setState("FINISHED");
    stats.setStartTimeInMS(1000L);
    stats.setEndTimeInMS(2000L);
    stats.setInitDataQuerySourceCost(500000L);
    stats.setSeqUnclosedNum(0);
    stats.setSeqClosednNum(5);
    stats.setUnseqUnclosedNum(1);
    stats.setUnseqClosedNum(3);
    stats.setReadyQueuedTime(100000L);
    stats.setBlockQueuedTime(50000L);

    TQueryStatistics queryStats = createTestQueryStatistics();
    stats.setQueryStatistics(queryStats);

    // Create operator statistics
    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("LimitOperator");
    opStats.setTotalExecutionTimeInNanos(300000L);
    opStats.setOutputRows(50);
    opStats.hasNextCalledCount = 100;
    opStats.nextCalledCount = 50;
    opStats.setMemoryUsage(1024);
    opStats.setCount(10);
    Map<String, TOperatorStatistics> operatorMap = new HashMap<>();
    operatorMap.put("node1", opStats);
    stats.setOperatorStatisticsMap(operatorMap);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals(1, root.get("fragmentInstancesCount").getAsInt());
    JsonArray instances = root.getAsJsonArray("fragmentInstances");
    assertEquals(1, instances.size());

    JsonObject fi = instances.get(0).getAsJsonObject();
    assertEquals("127.0.0.1", fi.get("ip").getAsString());
    assertEquals("root.db.region1", fi.get("dataRegion").getAsString());
    assertEquals("FINISHED", fi.get("state").getAsString());
    assertEquals(1000, fi.get("totalWallTimeMs").getAsLong());
    assertEquals(5, fi.get("seqFileClosed").getAsInt());
    assertEquals(3, fi.get("unseqFileClosed").getAsInt());

    // Verify operator tree
    assertTrue(fi.has("operators"));
    JsonObject operators = fi.getAsJsonObject("operators");
    assertEquals("LimitOperator", operators.get("operatorType").getAsString());
    assertEquals(50, operators.get("outputRows").getAsInt());
    assertEquals(1024, operators.get("estimatedMemorySize").getAsInt());
    assertEquals(10, operators.get("count").getAsInt());

    // Verify query statistics
    assertTrue(fi.has("queryStatistics"));
    JsonObject qs = fi.getAsJsonObject("queryStatistics");
    assertEquals(100, qs.get("timeSeriesIndexFilteredRows").getAsLong());
    assertEquals(50, qs.get("chunkIndexFilteredRows").getAsLong());
    assertEquals(25, qs.get("pageIndexFilteredRows").getAsLong());
  }

  @Test
  public void testRenderWithVerboseQueryStatistics() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    TFetchFragmentInstanceStatisticsResp stats = new TFetchFragmentInstanceStatisticsResp();
    stats.setDataRegion("root.db.region1");
    stats.setIp("127.0.0.1");
    stats.setState("FINISHED");
    stats.setStartTimeInMS(1000L);
    stats.setEndTimeInMS(2000L);
    stats.setInitDataQuerySourceCost(500000L);
    stats.setSeqUnclosedNum(0);
    stats.setSeqClosednNum(5);
    stats.setUnseqUnclosedNum(1);
    stats.setUnseqClosedNum(3);
    stats.setReadyQueuedTime(100000L);
    stats.setBlockQueuedTime(50000L);

    TQueryStatistics queryStats = createVerboseQueryStatistics();
    stats.setQueryStatistics(queryStats);
    stats.setOperatorStatisticsMap(new HashMap<>());

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    // Test with verbose=true to cover verbose code paths
    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, true);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    JsonObject fi = root.getAsJsonArray("fragmentInstances").get(0).getAsJsonObject();
    JsonObject qs = fi.getAsJsonObject("queryStatistics");

    // Verbose fields should be present
    assertTrue(qs.has("loadBloomFilterFromCacheCount"));
    assertTrue(qs.has("loadBloomFilterFromDiskCount"));
    assertTrue(qs.has("loadBloomFilterActualIOSize"));
    assertTrue(qs.has("loadBloomFilterTimeMs"));
    assertTrue(qs.has("loadTimeSeriesMetadataFromCacheCount"));
    assertTrue(qs.has("loadTimeSeriesMetadataFromDiskCount"));
    assertTrue(qs.has("loadTimeSeriesMetadataActualIOSize"));
    assertTrue(qs.has("loadChunkFromCacheCount"));
    assertTrue(qs.has("loadChunkFromDiskCount"));
    assertTrue(qs.has("loadChunkActualIOSize"));
    assertTrue(qs.has("rowScanFilteredRows"));

    // Non-zero disk seq counts should appear
    assertTrue(qs.has("loadTimeSeriesMetadataDiskSeqCount"));
    assertTrue(qs.has("loadTimeSeriesMetadataDiskUnSeqCount"));
  }

  @Test
  public void testRenderWithNullStatistics() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    // No statistics for this instance
    String json =
        drawer.renderFragmentInstancesAsJson(
            Collections.singletonList(instance), Collections.emptyMap(), false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    // Instance is filtered out because statistics is null
    assertEquals(0, root.get("fragmentInstancesCount").getAsInt());
  }

  @Test
  public void testRenderWithNullDataRegion() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    TFetchFragmentInstanceStatisticsResp stats = new TFetchFragmentInstanceStatisticsResp();
    // DataRegion is null -> instance should be filtered out
    stats.setIp("127.0.0.1");

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    // Instance is filtered out because dataRegion is null
    assertEquals(0, root.get("fragmentInstancesCount").getAsInt());
  }

  @Test
  public void testRenderOperatorWithSpecifiedInfo() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    TFetchFragmentInstanceStatisticsResp stats = createBasicStats();

    // Create operator with specifiedInfo
    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("TableScanOperator");
    opStats.setTotalExecutionTimeInNanos(500000L);
    opStats.setOutputRows(200);
    opStats.hasNextCalledCount = 50;
    opStats.nextCalledCount = 25;
    Map<String, String> specifiedInfo = new HashMap<>();
    specifiedInfo.put("tableName", "test_table");
    specifiedInfo.put("scanOrder", "ASC");
    opStats.setSpecifiedInfo(specifiedInfo);
    Map<String, TOperatorStatistics> operatorMap = new HashMap<>();
    operatorMap.put("node1", opStats);
    stats.setOperatorStatisticsMap(operatorMap);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    JsonObject fi = root.getAsJsonArray("fragmentInstances").get(0).getAsJsonObject();
    JsonObject operators = fi.getAsJsonObject("operators");
    assertTrue(operators.has("specifiedInfo"));
    JsonObject info = operators.getAsJsonObject("specifiedInfo");
    assertEquals("test_table", info.get("tableName").getAsString());
    assertEquals("ASC", info.get("scanOrder").getAsString());
  }

  @Test
  public void testRenderOperatorWithChildren() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    // Create a plan with children: Limit -> Exchange
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("child1"));
    LimitNode limitNode =
        new LimitNode(new PlanNodeId("parent"), exchangeNode, 100, Optional.empty());

    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), limitNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    TFetchFragmentInstanceStatisticsResp stats = createBasicStats();

    TOperatorStatistics parentOp = new TOperatorStatistics();
    parentOp.setOperatorType("LimitOperator");
    parentOp.setTotalExecutionTimeInNanos(300000L);
    parentOp.setOutputRows(50);
    parentOp.hasNextCalledCount = 100;
    parentOp.nextCalledCount = 50;

    TOperatorStatistics childOp = new TOperatorStatistics();
    childOp.setOperatorType("ExchangeOperator");
    childOp.setTotalExecutionTimeInNanos(200000L);
    childOp.setOutputRows(100);
    childOp.hasNextCalledCount = 80;
    childOp.nextCalledCount = 40;

    Map<String, TOperatorStatistics> operatorMap = new HashMap<>();
    operatorMap.put("parent", parentOp);
    operatorMap.put("child1", childOp);
    stats.setOperatorStatisticsMap(operatorMap);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    JsonObject fi = root.getAsJsonArray("fragmentInstances").get(0).getAsJsonObject();
    JsonObject operators = fi.getAsJsonObject("operators");
    assertEquals("LimitOperator", operators.get("operatorType").getAsString());
    assertTrue(operators.has("children"));
    JsonArray children = operators.getAsJsonArray("children");
    assertEquals(1, children.size());
    assertEquals(
        "ExchangeOperator", children.get(0).getAsJsonObject().get("operatorType").getAsString());
  }

  @Test
  public void testRenderWithRetryCount() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    LimitNode planNode = new LimitNode(new PlanNodeId("node1"), null, 100, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    FragmentInstance instance =
        new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);

    TFetchFragmentInstanceStatisticsResp stats = createBasicStats();
    stats.setInitDataQuerySourceRetryCount(3);
    stats.setOperatorStatisticsMap(new HashMap<>());

    TQueryStatistics queryStats = createTestQueryStatistics();
    stats.setQueryStatistics(queryStats);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, stats);

    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    JsonObject fi = root.getAsJsonArray("fragmentInstances").get(0).getAsJsonObject();
    assertTrue(fi.has("initDataQuerySourceRetryCount"));
    assertEquals(3, fi.get("initDataQuerySourceRetryCount").getAsInt());
  }

  @Test
  public void testJsonOutputIsValid() {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext context = createTestContext();
    context.setAnalyzeCost(1000000L);
    drawer.renderPlanStatistics(context);
    drawer.renderDispatchCost(context);

    String json =
        drawer.renderFragmentInstancesAsJson(
            Collections.emptyList(), Collections.emptyMap(), false);

    // Verify the JSON is pretty-printed (contains newlines and indentation)
    assertTrue(json.contains("\n"));
    assertTrue(json.contains("  "));

    // Verify it's valid JSON
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    assertNotNull(root);
    assertTrue(root.has("planStatistics"));
    assertTrue(root.has("fragmentInstancesCount"));
    assertTrue(root.has("fragmentInstances"));
  }

  private MPPQueryContext createTestContext() {
    QueryId queryId = new QueryId("test_query_id");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    MPPQueryContext context =
        new MPPQueryContext(
            "SELECT * FROM test",
            queryId,
            sessionInfo,
            new org.apache.iotdb.common.rpc.thrift.TEndPoint("127.0.0.1", 6667),
            new org.apache.iotdb.common.rpc.thrift.TEndPoint("127.0.0.1", 10730));
    // Initialize queryPlanStatistics to avoid NPE on getAnalyzeCost() etc.
    context.setAnalyzeCost(0);
    return context;
  }

  private TQueryStatistics createTestQueryStatistics() {
    TQueryStatistics qs = new TQueryStatistics();
    qs.timeSeriesIndexFilteredRows = 100;
    qs.chunkIndexFilteredRows = 50;
    qs.pageIndexFilteredRows = 25;
    qs.rowScanFilteredRows = 10;
    qs.loadTimeSeriesMetadataFromCacheCount = 0;
    qs.loadTimeSeriesMetadataFromDiskCount = 0;
    qs.loadTimeSeriesMetadataActualIOSize = 0;
    qs.loadChunkFromCacheCount = 0;
    qs.loadChunkFromDiskCount = 0;
    qs.loadChunkActualIOSize = 0;
    qs.loadBloomFilterFromCacheCount = 0;
    qs.loadBloomFilterFromDiskCount = 0;
    qs.loadBloomFilterActualIOSize = 0;
    qs.loadBloomFilterTime = 0;
    return qs;
  }

  private TQueryStatistics createVerboseQueryStatistics() {
    TQueryStatistics qs = new TQueryStatistics();
    qs.timeSeriesIndexFilteredRows = 100;
    qs.chunkIndexFilteredRows = 50;
    qs.pageIndexFilteredRows = 25;
    qs.rowScanFilteredRows = 10;

    // Bloom filter stats
    qs.loadBloomFilterFromCacheCount = 5;
    qs.loadBloomFilterFromDiskCount = 3;
    qs.loadBloomFilterActualIOSize = 1024;
    qs.loadBloomFilterTime = 500000; // 0.5ms

    // TimeSeries metadata stats
    qs.loadTimeSeriesMetadataDiskSeqCount = 10;
    qs.loadTimeSeriesMetadataDiskUnSeqCount = 5;
    qs.loadTimeSeriesMetadataMemSeqCount = 8;
    qs.loadTimeSeriesMetadataMemUnSeqCount = 3;
    qs.loadTimeSeriesMetadataDiskSeqTime = 1000000; // 1ms
    qs.loadTimeSeriesMetadataDiskUnSeqTime = 500000; // 0.5ms
    qs.loadTimeSeriesMetadataMemSeqTime = 200000;
    qs.loadTimeSeriesMetadataMemUnSeqTime = 100000;

    // Aligned metadata stats
    qs.loadTimeSeriesMetadataAlignedDiskSeqCount = 2;
    qs.loadTimeSeriesMetadataAlignedDiskUnSeqCount = 1;
    qs.loadTimeSeriesMetadataAlignedMemSeqCount = 4;
    qs.loadTimeSeriesMetadataAlignedMemUnSeqCount = 2;
    qs.loadTimeSeriesMetadataAlignedDiskSeqTime = 300000;
    qs.loadTimeSeriesMetadataAlignedDiskUnSeqTime = 150000;
    qs.loadTimeSeriesMetadataAlignedMemSeqTime = 100000;
    qs.loadTimeSeriesMetadataAlignedMemUnSeqTime = 50000;

    qs.loadTimeSeriesMetadataFromCacheCount = 20;
    qs.loadTimeSeriesMetadataFromDiskCount = 15;
    qs.loadTimeSeriesMetadataActualIOSize = 4096;

    // Chunk reader stats
    qs.constructNonAlignedChunkReadersDiskCount = 5;
    qs.constructNonAlignedChunkReadersMemCount = 3;
    qs.constructAlignedChunkReadersDiskCount = 2;
    qs.constructAlignedChunkReadersMemCount = 1;
    qs.constructNonAlignedChunkReadersDiskTime = 200000;
    qs.constructNonAlignedChunkReadersMemTime = 100000;
    qs.constructAlignedChunkReadersDiskTime = 150000;
    qs.constructAlignedChunkReadersMemTime = 75000;

    // Chunk stats
    qs.loadChunkFromCacheCount = 10;
    qs.loadChunkFromDiskCount = 8;
    qs.loadChunkActualIOSize = 2048;

    // Page reader stats
    qs.pageReadersDecodeAlignedDiskCount = 6;
    qs.pageReadersDecodeAlignedDiskTime = 300000;
    qs.pageReadersDecodeAlignedMemCount = 4;
    qs.pageReadersDecodeAlignedMemTime = 200000;
    qs.pageReadersDecodeNonAlignedDiskCount = 3;
    qs.pageReadersDecodeNonAlignedDiskTime = 150000;
    qs.pageReadersDecodeNonAlignedMemCount = 2;
    qs.pageReadersDecodeNonAlignedMemTime = 100000;
    qs.pageReaderMaxUsedMemorySize = 65536;
    qs.chunkWithMetadataErrorsCount = 1;

    return qs;
  }

  private TFetchFragmentInstanceStatisticsResp createBasicStats() {
    TFetchFragmentInstanceStatisticsResp stats = new TFetchFragmentInstanceStatisticsResp();
    stats.setDataRegion("root.db.region1");
    stats.setIp("127.0.0.1");
    stats.setState("FINISHED");
    stats.setStartTimeInMS(1000L);
    stats.setEndTimeInMS(2000L);
    stats.setInitDataQuerySourceCost(500000L);
    stats.setSeqUnclosedNum(0);
    stats.setSeqClosednNum(5);
    stats.setUnseqUnclosedNum(1);
    stats.setUnseqClosedNum(3);
    stats.setReadyQueuedTime(100000L);
    stats.setBlockQueuedTime(50000L);

    TQueryStatistics queryStats = createTestQueryStatistics();
    stats.setQueryStatistics(queryStats);
    return stats;
  }
}
