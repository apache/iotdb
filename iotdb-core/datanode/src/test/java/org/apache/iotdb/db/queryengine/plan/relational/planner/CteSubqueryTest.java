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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({Coordinator.class, SessionManager.class})
public class CteSubqueryTest {
  private PlanTester planTester;

  @Before
  public void setUp() throws Exception {
    planTester = new PlanTester();
    mockExecuteForTableModel();
  }

  /**
   * This test primarily ensures code coverage:
   * PredicateWithUncorrelatedScalarSubqueryReconstructor.fetchUncorrelatedSubqueryResultForPredicate
   */
  @Test
  public void testCteSubquery() throws IoTDBException {
    mockExecuteForTableModel();

    String sql =
        "with cte1 as (select time, s2 from table1) select s1 from table1 "
            + "where s1 = (select s2 from cte1)";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("s1"),
            ImmutableSet.of("s1"),
            new ComparisonExpression(EQUAL, new SymbolReference("s1"), new LongLiteral("1")));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──DeviceTableScanNode
     */
    assertPlan(logicalQueryPlan, output(tableScan));

    // Verify DistributionPlan
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));

    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  private void mockExecuteForTableModel() throws IoTDBException {
    mockStatic(Coordinator.class);
    mockStatic(SessionManager.class);

    // Create a mock Coordinator instance
    Coordinator mockCoordinator = Mockito.mock(Coordinator.class);
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

    // Create QueryExecution mock
    QueryExecution mockQueryExecution = Mockito.mock(QueryExecution.class);
    when(mockQueryExecution.hasNextResult())
        .thenReturn(true) // First call returns true
        .thenReturn(false); // Subsequent calls return false

    // Create a real DatasetHeader with time and s1 columns
    List<ColumnHeader> columnHeaders = ImmutableList.of(new ColumnHeader("s2", TSDataType.INT64));
    DatasetHeader mockDatasetHeader = new DatasetHeader(columnHeaders, false);
    when(mockQueryExecution.getDatasetHeader()).thenReturn(mockDatasetHeader);

    // Create a TSBlock with sample data for getBatchResult

    TimeColumn timeColumn = new TimeColumn(1, new long[] {1000L});
    LongColumn valueColumn = new LongColumn(1, Optional.empty(), new long[] {1L});
    TsBlock sampleTsBlock = new TsBlock(timeColumn, valueColumn);
    when(mockQueryExecution.getBatchResult()).thenReturn(Optional.of(sampleTsBlock));

    // Mock coordinator methods
    when(mockCoordinator.getQueryExecution(Mockito.anyLong())).thenReturn(mockQueryExecution);
  }
}
