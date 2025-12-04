/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.cteScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.explainAnalyze;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.offset;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.ASCENDING;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({Coordinator.class, SessionManager.class})
public class CteMaterializerTest {
  private static PlanTester planTester;
  private static Coordinator mockCoordinator;

  @BeforeClass
  public static void prepareEnv() {
    planTester = new PlanTester();
    mockStatic(Coordinator.class);
    mockStatic(SessionManager.class);

    // Create a mock Coordinator instance
    mockCoordinator = Mockito.mock(Coordinator.class);
    when(Coordinator.getInstance()).thenReturn(mockCoordinator);

    // Create mock SessionManager
    SessionManager mockSessionManager = Mockito.mock(SessionManager.class);
    when(SessionManager.getInstance()).thenReturn(mockSessionManager);

    // Mock TSStatus with success status
    TSStatus mockStatus = Mockito.mock(TSStatus.class);
    when(mockStatus.getCode()).thenReturn(200); // Success status code

    // Create a real ExecutionResult instance
    ExecutionResult mockResult = new ExecutionResult(new QueryId("1"), mockStatus);

    // Mock the executeForTableModel method
    when(mockCoordinator.executeForTableModel(
            Mockito.any(), // Statement
            Mockito.any(), // SqlParser
            Mockito.any(), // IClientSession
            Mockito.anyLong(), // queryId
            Mockito.any(), // SessionInfo
            Mockito.anyString(), // String
            Mockito.any(), // Metadata
            Mockito.anyMap(), // Map<NodeRef<Table>, CteDataStore>
            Mockito.any(), // ExplainType
            Mockito.anyLong(), // timeOut
            Mockito.anyBoolean())) // userQuery
        .thenReturn(mockResult);
  }

  @Before
  public void setUp() throws IoTDBException {
    // Create QueryExecution mock
    QueryExecution mockQueryExecution = Mockito.mock(QueryExecution.class);
    when(mockQueryExecution.hasNextResult())
        .thenReturn(true) // First call returns true
        .thenReturn(false); // Subsequent calls return false

    // Create a real DatasetHeader with time and s1 columns
    List<ColumnHeader> columnHeaders =
        ImmutableList.of(
            new ColumnHeader("time", TSDataType.TIMESTAMP),
            new ColumnHeader("s1", TSDataType.INT64));
    DatasetHeader mockDatasetHeader = new DatasetHeader(columnHeaders, false);
    when(mockQueryExecution.getDatasetHeader()).thenReturn(mockDatasetHeader);

    // Create a TSBlock with sample data for getBatchResult
    long[] timestamps = {1000L, 2000L, 3000L};
    long[] values = {10L, 20L, 30L};
    TimeColumn timeColumn = new TimeColumn(3, timestamps);
    LongColumn valueColumn = new LongColumn(3, Optional.empty(), values);
    TsBlock sampleTsBlock = new TsBlock(timeColumn, valueColumn);
    when(mockQueryExecution.getBatchResult()).thenReturn(Optional.of(sampleTsBlock));

    // Mock coordinator methods
    when(mockCoordinator.getQueryExecution(Mockito.anyLong())).thenReturn(mockQueryExecution);
  }

  private void mockException() {
    CteMaterializer cteMaterializer = Mockito.spy(new CteMaterializer());
    Mockito.doReturn(null)
        .when(cteMaterializer)
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    CteMaterializer.setInstance(cteMaterializer);
  }

  @Test
  public void testSimpleCte() {
    String sql = "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(cteScan));
  }

  @Test
  public void testFieldFilterCte() {
    String sql =
        "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1 where s1 > 10";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    Expression filterPredicate =
        new ComparisonExpression(GREATER_THAN, new SymbolReference("s1"), new LongLiteral("10"));
    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──FilterNode
     *         └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(filter(filterPredicate, cteScan)));
  }

  @Test
  public void testTimeFilterCte() {
    String sql =
        "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1 where time > 1000";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    Expression filterPredicate =
        new ComparisonExpression(
            GREATER_THAN, new SymbolReference("time"), new LongLiteral("1000"));
    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──FilterNode
     *         └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(filter(filterPredicate, cteScan)));
  }

  @Test
  public void testSortCte() {
    String sql =
        "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1 order by s1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("s1", ASCENDING, LAST));
    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──SortNode
     *         └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(sort(orderBy, cteScan)));
  }

  @Test
  public void testLimitOffsetCte() {
    String sql =
        "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1 limit 1 offset 2";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──LimitNode
     *         └──OffsetNode
     *            └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(offset(2, limit(3, cteScan))));
  }

  @Test
  public void testAggCte() {
    String sql =
        "with cte1 as materialized (SELECT time, s1 FROM table1) select s1, max(time) from cte1 group by s1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──AggregationNode
     *         └──CteScanNode
     */
    assertPlan(
        logicalQueryPlan,
        output(
            aggregation(
                singleGroupingSet("s1"),
                ImmutableMap.of(
                    Optional.of("max"), aggregationFunction("max", ImmutableList.of("time"))),
                Collections.emptyList(),
                Optional.empty(),
                SINGLE,
                cteScan)));
  }

  @Test
  public void testCteQueryException() {
    CteMaterializer originalCteMaterializer = CteMaterializer.getInstance();
    mockException();

    String sql = "with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("time", "s1"), ImmutableSet.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(tableScan));

    // reset original CteMaterializer
    CteMaterializer.setInstance(originalCteMaterializer);
  }

  @Test
  public void testExplainAnalyze() {
    String sql =
        "explain analyze with cte1 as materialized (SELECT time, s1 FROM table1) select * from cte1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──ExplainAnalyzeNode
     *         └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(explainAnalyze(cteScan)));
  }

  /**
   * This test primarily ensures code coverage: materializeCTE.handleCteExplainResults &
   * materializeCTE.fetchCteQueryResult
   */
  @Test
  public void testExplain() {
    String sql =
        "with cte1 as (select time, s1 from table1), "
            + "cte2 as materialized (select * from cte1) select * from cte2";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql, true);

    PlanMatchPattern cteScan = cteScan("cte2", ImmutableList.of("time", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──CteScanNode
     */
    assertPlan(logicalQueryPlan, output(cteScan));
  }
}
