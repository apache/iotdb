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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.add;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.function;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;

/** Use optimize rule: LimitOffsetPushDown and OrderByExpressionWithLimitChangeToTopK */
public class LimitOffsetPushDownTest {

  @Test
  public void testNonAlignedPushDown() {
    checkPushDown(
        "select s1 from root.sg.d1 limit 100;",
        new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).limit("1", 100).getRoot(),
        new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1"), 100, 0).getRoot());
    checkPushDown(
        "select s1 from root.sg.d1 offset 100;",
        new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).offset("1", 100).getRoot(),
        new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1"), 0, 100).getRoot());
    checkPushDown(
        "select s1 from root.sg.d1 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .offset("1", 100)
            .limit("2", 100)
            .getRoot(),
        new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100).getRoot());
  }

  @Test
  public void testAlignedPushDown() {
    checkPushDown(
        "select s1, s2 from root.sg.d2.a limit 100;",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .limit("1", 100)
            .getRoot(),
        new TestPlanBuilder().scanAligned("0", schemaMap.get("root.sg.d2.a"), 100, 0).getRoot());
    checkPushDown(
        "select s1, s2 from root.sg.d2.a offset 100;",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .offset("1", 100)
            .getRoot(),
        new TestPlanBuilder().scanAligned("0", schemaMap.get("root.sg.d2.a"), 0, 100).getRoot());
    checkPushDown(
        "select s1, s2 from root.sg.d2.a limit 100 offset 100;",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .offset("1", 100)
            .limit("2", 100)
            .getRoot(),
        new TestPlanBuilder().scanAligned("0", schemaMap.get("root.sg.d2.a"), 100, 100).getRoot());
  }

  @Test
  public void testPushDownWithTransform() {
    List<Expression> expressions =
        Arrays.asList(
            add(
                function("sin", add(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("1"))),
                intValue("1")),
            gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")));
    checkPushDown(
        "select sin(s1 + 1) + 1, s1 > 10 from root.sg.d1 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .transform("1", expressions)
            .offset("2", 100)
            .limit("3", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100)
            .transform("1", expressions)
            .getRoot());
  }

  @Test
  public void testPushDownWithFill() {
    checkPushDown(
        "select s1 from root.sg.d1 fill(100) limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .fill("1", "100")
            .offset("2", 100)
            .limit("3", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100)
            .fill("1", "100")
            .getRoot());
  }

  @Test
  public void testPushDownAlignByDevice() {
    // non aligned device
    checkPushDown(
        "select s1 from root.sg.d1 limit 100 offset 100 align by device;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .singleDeviceView("1", "root.sg.d1", "s1")
            .offset("2", 100)
            .limit("3", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100)
            .singleDeviceView("1", "root.sg.d1", "s1")
            .getRoot());

    OrderByParameter orderByParameter =
        new OrderByParameter(
            Arrays.asList(
                new SortItem(OrderByKey.TIME, Ordering.ASC),
                new SortItem(OrderByKey.DEVICE, Ordering.ASC)));
    checkPushDown(
        "select s1 from root.sg.d1 order by time asc limit 100 offset 100 align by device;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 200)
            .singleOrderedDeviceView("1", "root.sg.d1", orderByParameter, "s1")
            .offset("2", 100)
            .limit("3", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100)
            .singleOrderedDeviceView("1", "root.sg.d1", orderByParameter, "s1")
            .getRoot());

    // can not push down
    orderByParameter =
        new OrderByParameter(
            Arrays.asList(
                new SortItem("s1", Ordering.ASC),
                new SortItem("DEVICE", Ordering.ASC),
                new SortItem("TIME", Ordering.ASC)));
    checkPushDown(
        "select s1 from root.sg.d1 order by s1 asc limit 100 offset 100 align by device;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .singleOrderedDeviceView("1", "root.sg.d1", orderByParameter, "s1")
            .sort("2", orderByParameter)
            .offset("3", 100)
            .limit("4", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .singleOrderedDeviceView("1", "root.sg.d1", orderByParameter, "s1")
            .topK("5", 200, orderByParameter, Arrays.asList("Device", "s1"))
            .offset("3", 100)
            .limit("4", 100)
            .getRoot());

    orderByParameter =
        new OrderByParameter(
            Arrays.asList(
                new SortItem("s1", Ordering.ASC),
                new SortItem("DEVICE", Ordering.ASC),
                new SortItem("TIME", Ordering.ASC)));
    checkPushDown(
        "select s1,s2 from root.sg.d2.a order by s1 asc limit 100 offset 100 align by device;",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .singleOrderedDeviceView("1", "root.sg.d2.a", orderByParameter, "s1", "s2")
            .sort("2", orderByParameter)
            .offset("3", 100)
            .limit("4", 100)
            .getRoot(),
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .singleOrderedDeviceView("1", "root.sg.d2.a", orderByParameter, "s1", "s2")
            .topK("5", 200, orderByParameter, Arrays.asList("Device", "s1"))
            .offset("3", 100)
            .limit("4", 100)
            .getRoot());
  }

  @Test
  public void testPushDownWithInto() {
    checkPushDown(
        "select s1 into root.sg.d2(s1) from root.sg.d1 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .offset("1", 100)
            .limit("2", 100)
            .into("3", schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d2.s1"))
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"), 100, 100)
            .into("3", schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d2.s1"))
            .getRoot());
  }

  @Test
  public void testCannotPushDown() {
    checkCannotPushDown(
        "select s1, s2 from root.sg.d1 limit 100;",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .limit("3", 100)
            .getRoot());

    checkCannotPushDown(
        "select diff(s1 + 1) + 1 from root.sg.d1 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .transform(
                "1",
                Collections.singletonList(
                    add(
                        function(
                            "diff", add(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("1"))),
                        intValue("1"))))
            .offset("2", 100)
            .limit("3", 100)
            .getRoot());

    LinkedHashMap<String, String> functionAttributes = new LinkedHashMap<>();
    functionAttributes.put("windowSize", "10");
    checkCannotPushDown(
        "select m4(s1,'windowSize'='10') from root.sg.d1 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .transform(
                "1",
                Collections.singletonList(
                    function("m4", functionAttributes, timeSeries(schemaMap.get("root.sg.d1.s1")))))
            .offset("2", 100)
            .limit("3", 100)
            .getRoot());

    checkCannotPushDown(
        "select s1 from root.sg.d1 fill(linear) limit 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .fill("1", FillPolicy.LINEAR)
            .limit("2", 100)
            .getRoot());
    checkCannotPushDown(
        "select s1 from root.sg.d1 fill(previous) limit 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .fill("1", FillPolicy.PREVIOUS)
            .limit("2", 100)
            .getRoot());

    checkCannotPushDown(
        "select s1 from root.sg.d1 where s1 > 10 limit 100 offset 100;",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .filter(
                "1",
                Collections.singletonList(timeSeries(schemaMap.get("root.sg.d1.s1"))),
                gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                false)
            .offset("2", 100)
            .limit("3", 100)
            .getRoot());
  }

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    OptimizationTestUtil.checkPushDown(
        Collections.emptyList(), new LimitOffsetPushDown(), sql, rawPlan, optPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    OptimizationTestUtil.checkCannotPushDown(
        Collections.emptyList(), new LimitOffsetPushDown(), sql, rawPlan);
  }

  // test for limit/offset push down in group by time
  @Test
  public void testGroupByTimePushDown() {
    String sql = "select avg(s1),sum(s2) from root.** group by ((1, 899], 200ms) offset 1 limit 2";
    checkGroupByTimePushDown(sql, 201, 601, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown2() {
    String sql = "select avg(s1),sum(s2) from root.** group by ([4, 899), 200ms) offset 2 limit 3";
    checkGroupByTimePushDown(sql, 404, 899, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown3() {
    String sql = "select avg(s1),sum(s2) from root.** group by ([4, 899), 88ms) offset 2 limit 3";
    checkGroupByTimePushDown(sql, 180, 444, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown4() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 88ms) order by time offset 2 limit 3";
    checkGroupByTimePushDown(sql, 180, 444, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown5() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 88ms) order by time desc offset 2 limit 3";
    checkGroupByTimePushDown(sql, 532, 796, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown6() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 100ms) order by time desc offset 2 limit 3";
    checkGroupByTimePushDown(sql, 404, 704, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown7() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 50ms) order by time desc offset 2 limit 3";
    checkGroupByTimePushDown(sql, 654, 804, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown8() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([0, 900), 100ms) order by time desc offset 2 limit 2";
    checkGroupByTimePushDown(sql, 500, 700, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown9() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 50ms) order by s1 offset 2 limit 3";
    checkGroupByTimePushDown(sql, 4, 899, 3, 2);
  }

  @Test
  public void testGroupByTimePushDown10() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 50ms, 25ms) offset 2 limit 3";
    checkGroupByTimePushDown(sql, 54, 154, 3, 0);
  }

  @Test
  public void testGroupByTimePushDown11() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 50ms, 75ms) offset 2 limit 3";
    checkGroupByTimePushDown(sql, 154, 354, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown12() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 200ms) order by time desc limit 3";
    checkGroupByTimePushDown(sql, 404, 899, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown13() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 200ms) order by time desc limit 5";
    checkGroupByTimePushDown(sql, 4, 899, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown14() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 200ms) order by time desc offset 2 limit 5";
    checkGroupByTimePushDown(sql, 4, 899, 0, 0);
  }

  @Test
  public void testGroupByTimePushDown15() {
    String sql =
        "select avg(s1),sum(s2) from root.** group by ([4, 899), 200ms) order by time desc limit 6";
    checkGroupByTimePushDown(sql, 4, 899, 0, 0);
  }

  private void checkGroupByTimePushDown(
      String sql, long startTime, long endTime, long rowLimit, long rowOffset) {
    QueryStatement queryStatement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    Assert.assertEquals(rowLimit, queryStatement.getRowLimit());
    Assert.assertEquals(rowOffset, queryStatement.getRowOffset());

    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    Assert.assertEquals(startTime, groupByTimeComponent.getStartTime());
    Assert.assertEquals(endTime, groupByTimeComponent.getEndTime());
  }

  // device: [root.sg.s1, root.sg.s2, root.sg.s2.a]
  private void checkGroupByTimePushDownInAlignByDevice(
      String sql,
      List<String> deviceSet,
      long rowLimit,
      long rowOffset,
      long startTime,
      long endTime) {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    Assert.assertEquals(rowLimit, statement.getRowLimit());
    Assert.assertEquals(rowOffset, statement.getRowOffset());

    int index = 0;
    List<PartialPath> deviceSetInAnalysis = analysis.getDeviceList();
    for (PartialPath path : deviceSetInAnalysis) {
      Assert.assertEquals(path.getFullPath(), deviceSet.get(index));
      index++;
    }

    GroupByTimeParameter groupByTimeParameter = analysis.getGroupByTimeParameter();
    Assert.assertEquals(startTime, groupByTimeParameter.getStartTime());
    Assert.assertEquals(endTime, groupByTimeParameter.getEndTime());
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice() {
    String sql =
        "select avg(s1) from root.** group by ([4, 899), 50ms) offset 16 limit 2 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d1");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 0, 0, 804, 899);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice2() {
    String sql =
        "select avg(s1) from root.** group by ([4, 899), 50ms) offset 16 limit 10 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d1");
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 10, 16, 4, 899);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice3() {
    String sql =
        "select avg(s1) from root.** group by ([4, 899), 50ms) offset 20 limit 2 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 0, 0, 104, 204);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice4() {
    String sql =
        "select avg(s1) from root.** group by ([4, 899), 50ms) offset 33 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d2.a");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 15, 4, 899);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice5() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) offset 9 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 0, 29, 179);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice6() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) offset 9 limit 9 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d2.a");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 9, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice7() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) offset 9 limit 9 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d2.a");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 9, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice8() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device offset 9 limit 9 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d2.a");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 9, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice9() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device desc offset 9 limit 9 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d1");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 9, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice10() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device, time desc offset 9 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 0, 54, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice11() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device desc, time desc offset 9 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 0, 54, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice12() {
    String sql =
        "select avg(s1) from root.** group by ([4, 899), 50ms) order by device desc offset 16 limit 2 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2.a");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 0, 0, 804, 899);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice13() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device, avg(s1) desc offset 9 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice14() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device, avg(s1) desc,time desc offset 9 limit 5 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 5, 1, 4, 199);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice15() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device desc limit 1 offset 8 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 1, 0, 4, 54);
  }

  @Test
  public void testGroupByTimePushDownInAlignByDevice16() {
    String sql =
        "select avg(s1) from root.** group by ([4, 199), 50ms, 25ms) order by device desc offset 8 align by device";
    List<String> deviceSet = new ArrayList<>();
    deviceSet.add("root.sg.d2");
    deviceSet.add("root.sg.d1");
    checkGroupByTimePushDownInAlignByDevice(sql, deviceSet, 0, 0, 4, 199);
  }
}
