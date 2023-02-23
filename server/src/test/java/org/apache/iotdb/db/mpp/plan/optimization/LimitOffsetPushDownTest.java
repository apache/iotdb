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

package org.apache.iotdb.db.mpp.plan.optimization;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.Analyzer;
import org.apache.iotdb.db.mpp.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.mpp.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.add;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.function;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.timeSeries;

public class LimitOffsetPushDownTest {

  private static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE));

      MeasurementPath aS1 = new MeasurementPath("root.sg.d2.a.s1", TSDataType.INT32);
      MeasurementPath aS2 = new MeasurementPath("root.sg.d2.a.s2", TSDataType.DOUBLE);
      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s1", "s2"),
              Arrays.asList(aS1.getMeasurementSchema(), aS2.getMeasurementSchema()));
      aS1.setUnderAlignedEntity(true);
      aS2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s1", aS1);
      schemaMap.put("root.sg.d2.a.s2", aS2);
      schemaMap.put("root.sg.d2.a", alignedPath);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

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
            .timeJoin(Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
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
                gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")))
            .offset("2", 100)
            .limit("3", 100)
            .getRoot());
  }

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    PlanNode actualPlan = planner.plan(analysis).getRootNode();
    Assert.assertEquals(rawPlan, actualPlan);

    PlanNode actualOptPlan = new LimitOffsetPushDown().optimize(actualPlan, analysis, context);
    Assert.assertEquals(optPlan, actualOptPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    PlanNode actualPlan = planner.plan(analysis).getRootNode();

    Assert.assertEquals(rawPlan, actualPlan);
    Assert.assertEquals(
        actualPlan, new LimitOffsetPushDown().optimize(actualPlan, analysis, context));
  }
}
