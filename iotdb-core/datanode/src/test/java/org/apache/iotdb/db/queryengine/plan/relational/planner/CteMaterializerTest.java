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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.cteScan;
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

public class CteMaterializerTest {
  private PlanTester planTester;

  @Before
  public void setUp() {
    planTester = new PlanTester();
    mockSubquery();
  }

  private Type convertType(String columnName) {
    switch (columnName) {
      case "time":
        return TypeFactory.getType(TSDataType.TIMESTAMP);
      case "s1":
      case "a1":
        return TypeFactory.getType(TSDataType.INT64);
      default:
    }
    return null;
  }

  private void mockException() {
    CteMaterializer cteMaterializer = Mockito.spy(new CteMaterializer());
    Mockito.doReturn(null)
        .when(cteMaterializer)
        .fetchCteQueryResult(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    CteMaterializer.setInstance(cteMaterializer);
  }

  private void mockSubquery() {
    CteMaterializer cteMaterializer = Mockito.spy(new CteMaterializer());

    Mockito.doAnswer(
            (InvocationOnMock invocation) -> {
              Table table = invocation.getArgument(1);
              Query query = invocation.getArgument(2);
              List<SelectItem> selectItems =
                  ((QuerySpecification) query.getQueryBody()).getSelect().getSelectItems();
              List<ColumnSchema> columnsSchemas =
                  selectItems.stream()
                      .map(
                          selectItem -> {
                            SingleColumn column = ((SingleColumn) selectItem);
                            String columnName =
                                column.getAlias().isPresent()
                                    ? column.getAlias().get().toString()
                                    : column.getExpression().toString();
                            return new ColumnSchema(
                                columnName,
                                convertType(columnName),
                                false,
                                TsTableColumnCategory.FIELD);
                          })
                      .collect(Collectors.toList());

              TableSchema tableSchema = new TableSchema(table.getName().toString(), columnsSchemas);
              return new CteDataStore(query, tableSchema, ImmutableList.of(0, 1));
            })
        .when(cteMaterializer)
        .fetchCteQueryResult(
            Mockito.any(MPPQueryContext.class),
            Mockito.any(Table.class),
            Mockito.any(Query.class),
            Mockito.any(With.class));
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
        "with cte1 as materialized (SELECT time, s1 as a1 FROM table1) select * from cte1 where a1 > 10";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    Expression filterPredicate =
        new ComparisonExpression(GREATER_THAN, new SymbolReference("a1"), new LongLiteral("10"));
    PlanMatchPattern cteScan = cteScan("cte1", ImmutableList.of("time", "a1"));

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
  }
}
