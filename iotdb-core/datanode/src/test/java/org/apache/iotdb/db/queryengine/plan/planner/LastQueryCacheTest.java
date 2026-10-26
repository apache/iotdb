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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DeviceLastCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.TimePredicate;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class LastQueryCacheTest {

  private static final String DATABASE = "root.last_query_cache";
  private final boolean aligned;
  private final Filter timeFilter;
  private final boolean updateSchema;
  private final boolean updateNullEntry;
  private final DataNodeSchemaCache cache = DataNodeSchemaCache.getInstance();
  private MeasurementPath path;

  @Parameterized.Parameters(name = "aligned={0}, filter={1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {false, null, true, true},
        new Object[] {true, null, true, true},
        new Object[] {false, TimeFilterApi.gt(10), true, false},
        new Object[] {true, TimeFilterApi.gt(10), true, false},
        new Object[] {false, TimeFilterApi.gtEq(10), true, false},
        new Object[] {true, TimeFilterApi.gtEq(10), true, false},
        new Object[] {false, TimeFilterApi.lt(10), false, false},
        new Object[] {true, TimeFilterApi.lt(10), false, false});
  }

  public LastQueryCacheTest(
      boolean aligned, Filter timeFilter, boolean updateSchema, boolean updateNullEntry) {
    this.aligned = aligned;
    this.timeFilter = timeFilter;
    this.updateSchema = updateSchema;
    this.updateNullEntry = updateNullEntry;
  }

  @Before
  public void setUp() throws Exception {
    cache.cleanUp();
    path = new MeasurementPath(DATABASE + ".d.s", TSDataType.INT32);
    path.setUnderAlignedEntity(aligned);
  }

  @After
  public void tearDown() {
    cache.cleanUp();
  }

  @Test
  public void testVirtualRegionDoesNotPopulateColdCache() throws Exception {
    LocalExecutionPlanContext context = createContext(VirtualDataRegion.getInstance());
    try (Operator operator = createOperator(context)) {
      // Initialization alone used to cache the virtual database as the device's owner.
      assertTrue(cache.getMatchedNormalSchema(path).isEmpty());
      consumeEmptyResult(operator, context);
      assertTrue(cache.getMatchedNormalSchema(path).isEmpty());
      assertNull(cache.getLastCache(path));
    }
    putSchema();
    assertEquals(DATABASE, cache.getMatchedNormalSchema(path).getBelongedDatabase(path));
  }

  @Test
  public void testVirtualRegionDoesNotUpdateWarmCache() throws Exception {
    putSchema();
    cache.declareLastCache(DATABASE, path);
    LocalExecutionPlanContext context = createContext(VirtualDataRegion.getInstance());
    try (Operator operator = createOperator(context)) {
      consumeEmptyResult(operator, context);
      assertEquals(DATABASE, cache.getMatchedNormalSchema(path).getBelongedDatabase(path));
      // An empty virtual scan must not turn a pending cache entry into a cached empty result.
      assertNull(cache.getLastCache(path));
    }
  }

  @Test
  public void testRealRegionRetainsEmptyResultCachePolicy() throws Exception {
    IDataRegionForQuery dataRegion = mock(IDataRegionForQuery.class);
    when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    LocalExecutionPlanContext context = createContext(dataRegion);
    try (Operator operator = createOperator(context)) {
      if (updateSchema) {
        assertEquals(DATABASE, cache.getMatchedNormalSchema(path).getBelongedDatabase(path));
      } else {
        assertTrue(cache.getMatchedNormalSchema(path).isEmpty());
      }
      consumeEmptyResult(operator, context);
      if (updateNullEntry) {
        assertSame(DeviceLastCache.EMPTY_TIME_VALUE_PAIR, cache.getLastCache(path));
      } else {
        assertNull(cache.getLastCache(path));
      }
    }
  }

  private void putSchema() {
    ClusterSchemaTree tree = new ClusterSchemaTree();
    tree.appendSingleMeasurementPath(path);
    tree.setDatabases(Collections.singleton(DATABASE));
    cache.put(tree);
  }

  private LocalExecutionPlanContext createContext(IDataRegionForQuery dataRegion) {
    QueryId queryId = new QueryId("last_query_cache_test");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "instance");
    DataNodeQueryContext queryContext = new DataNodeQueryContext(1);
    HashMap<QueryId, DataNodeQueryContext> queryContexts = new HashMap<>();
    queryContexts.put(queryId, queryContext);
    FragmentInstanceContext instanceContext =
        spy(
            createFragmentInstanceContext(
                instanceId,
                new FragmentInstanceStateMachine(instanceId, Runnable::run),
                new SessionInfo(1, "test", ZoneId.systemDefault(), "127.0.0.1"),
                dataRegion,
                (TimePredicate) null,
                queryContexts,
                false));
    when(instanceContext.getGlobalTimeFilter()).thenReturn(timeFilter);
    return new LocalExecutionPlanContext(new TypeProvider(), instanceContext, queryContext);
  }

  private Operator createOperator(LocalExecutionPlanContext context) {
    LastQueryNode node = new LastQueryNode(new PlanNodeId("last"), null, false);
    node.addDeviceLastQueryScanNode(
        new PlanNodeId("scan"),
        new PartialPath(path.getDevicePath().getNodes()),
        aligned,
        Collections.singletonList(path.getMeasurementSchema()),
        null,
        false);
    return node.accept(new OperatorTreeGenerator(), context);
  }

  private void consumeEmptyResult(Operator operator, LocalExecutionPlanContext context)
      throws Exception {
    context
        .getDriverContext()
        .getOperatorContexts()
        .forEach(operatorContext -> operatorContext.setMaxRunTime(TEST_TIME_SLICE));
    for (DataSourceOperator source :
        ((DataDriverContext) context.getDriverContext()).getSourceOperators()) {
      source.initQueryDataSource(
          new QueryDataSource(Collections.emptyList(), Collections.emptyList()));
    }
    int iterations = 0;
    while (operator.hasNext()) {
      assertTrue("Empty LAST scan did not finish", iterations++ < 10);
      assertTrue(operator.isBlocked().isDone());
      TsBlock block = operator.next();
      assertTrue(block == null || block.isEmpty());
    }
  }
}
