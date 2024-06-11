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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.add;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.or;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.sin;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;

public class PredicatePushDownTest {

  @Test
  public void testPushDownAlignByTime() {
    checkPushDown(
        "select s1, s2 from root.sg.d1 where time > 100 and s1 > 10",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d1.s1")),
                    timeSeries(schemaMap.get("root.sg.d1.s2"))),
                gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "4",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "0",
                        schemaMap.get("root.sg.d1.s1"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("1", schemaMap.get("root.sg.d1.s2")).getRoot())
            .getRoot());

    checkPushDown(
        "select s1 from root.sg.d1 where time > 100 and s2 > 10",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Collections.singletonList(timeSeries(schemaMap.get("root.sg.d1.s1"))),
                gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "4",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "1",
                        schemaMap.get("root.sg.d1.s2"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).getRoot())
            .project("5", Collections.singletonList("root.sg.d1.s1"))
            .getRoot());

    checkPushDown(
        "select sin(s1) from root.sg.d1 where time > 100 and s2 > 10",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Collections.singletonList(sin(timeSeries(schemaMap.get("root.sg.d1.s1")))),
                gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "4",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "1",
                        schemaMap.get("root.sg.d1.s2"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).getRoot())
            .transform(
                "5",
                Collections.singletonList(sin(timeSeries(schemaMap.get("root.sg.d1.s1")))),
                false)
            .getRoot());

    checkPushDown(
        "select sin(s1) from root.sg.d1 where time > 100 and s2 > 10 and s1 + s2 > 20",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Collections.singletonList(sin(timeSeries(schemaMap.get("root.sg.d1.s1")))),
                and(
                    gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")),
                    gt(
                        add(
                            timeSeries(schemaMap.get("root.sg.d1.s1")),
                            timeSeries(schemaMap.get("root.sg.d1.s2"))),
                        intValue("20"))),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "4",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "1",
                        schemaMap.get("root.sg.d1.s2"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).getRoot())
            .filter(
                "5",
                Collections.singletonList(sin(timeSeries(schemaMap.get("root.sg.d1.s1")))),
                gt(
                    add(
                        timeSeries(schemaMap.get("root.sg.d1.s1")),
                        timeSeries(schemaMap.get("root.sg.d1.s2"))),
                    intValue("20")),
                false)
            .getRoot());

    checkPushDown(
        "select s1, s2 from root.sg.d1 where time > 100 and s1 > 10 and s2 > 10",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d1.s1")),
                    timeSeries(schemaMap.get("root.sg.d1.s2"))),
                and(
                    gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10"))),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .innerTimeJoin(
                "4",
                Ordering.ASC,
                Arrays.asList("0", "1"),
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")),
                Arrays.asList(
                    gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10"))))
            .getRoot());

    checkPushDown(
        "select s1, s2 from root.sg.d2.a where time > 100 and s1 > 10",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .filter(
                "1",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                    timeSeries(schemaMap.get("root.sg.d2.a.s2"))),
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .scanAligned(
                "0",
                schemaMap.get("root.sg.d2.a"),
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")))
            .getRoot());

    checkPushDown(
        "select s1 from root.sg.d2.a where time > 100 and s2 > 10",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .filter(
                "1",
                Collections.singletonList(timeSeries(schemaMap.get("root.sg.d2.a.s1"))),
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s2")), intValue("10")),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .scanAligned(
                "0",
                schemaMap.get("root.sg.d2.a"),
                gt(timeSeries(schemaMap.get("root.sg.d2.a.s2")), intValue("10")))
            .project("2", Collections.singletonList("root.sg.d2.a.s1"))
            .getRoot());

    checkPushDown(
        "select s1, s2 from root.sg.d2.a where time > 100 and (s1 > 10 or s2 > 10)",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("root.sg.d2.a"))
            .filter(
                "1",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                    timeSeries(schemaMap.get("root.sg.d2.a.s2"))),
                or(
                    gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d2.a.s2")), intValue("10"))),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .scanAligned(
                "0",
                schemaMap.get("root.sg.d2.a"),
                or(
                    gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d2.a.s2")), intValue("10"))))
            .getRoot());

    checkPushDown(
        "select s1, s2 from root.sg.* where time > 100 and s1 > 10",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(
                    schemaMap.get("root.sg.d2.s1"),
                    schemaMap.get("root.sg.d1.s1"),
                    schemaMap.get("root.sg.d2.s2"),
                    schemaMap.get("root.sg.d1.s2")))
            .filter(
                "5",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d2.s1")),
                    timeSeries(schemaMap.get("root.sg.d1.s1")),
                    timeSeries(schemaMap.get("root.sg.d2.s2")),
                    timeSeries(schemaMap.get("root.sg.d1.s2"))),
                and(
                    gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d2.s1")), intValue("10"))),
                false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "8",
                Ordering.ASC,
                new TestPlanBuilder()
                    .innerTimeJoin(
                        "6",
                        Ordering.ASC,
                        Arrays.asList("0", "1"),
                        Arrays.asList(
                            schemaMap.get("root.sg.d2.s1"), schemaMap.get("root.sg.d1.s1")),
                        Arrays.asList(
                            gt(timeSeries(schemaMap.get("root.sg.d2.s1")), intValue("10")),
                            gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10"))))
                    .getRoot(),
                new TestPlanBuilder()
                    .fullOuterTimeJoin(
                        "7",
                        Ordering.ASC,
                        Arrays.asList("2", "3"),
                        Arrays.asList(
                            schemaMap.get("root.sg.d2.s2"), schemaMap.get("root.sg.d1.s2")))
                    .getRoot())
            .getRoot());
  }

  @Test
  public void testCannotPushDownAlignByTime() {
    checkCannotPushDown(
        "select s1, s2 from root.sg.d1 where time > 100 and (s1 > 10 or s2 > 10)",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .filter(
                "3",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d1.s1")),
                    timeSeries(schemaMap.get("root.sg.d1.s2"))),
                or(
                    gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10"))),
                false)
            .getRoot());

    checkCannotPushDown(
        "select a.s1, a.s2 from root.sg.d2 where time > 100 and (a.s1 > 10 or s2 > 10)",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                "2",
                Ordering.ASC,
                Arrays.asList("0", "1"),
                Arrays.asList(schemaMap.get("root.sg.d2.s2"), schemaMap.get("root.sg.d2.a")))
            .filter(
                "3",
                Arrays.asList(
                    timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                    timeSeries(schemaMap.get("root.sg.d2.a.s2"))),
                or(
                    gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                    gt(timeSeries(schemaMap.get("root.sg.d2.s2")), intValue("10"))),
                false)
            .getRoot());
  }

  @Test
  public void testPushDownAlignByDevice() {

    List<String> outputColumnNames = Arrays.asList("Device", "s1", "s2");
    List<String> devices = Arrays.asList("root.sg.d1", "root.sg.d2", "root.sg.d2.a");
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(1, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 2));

    checkPushDown(
        "select s1, s2 from root.sg.** where time > 100 and s1 > 10 align by device",
        new TestPlanBuilder()
            .deviceView(
                "10",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .fullOuterTimeJoin(
                            "2",
                            Ordering.ASC,
                            Arrays.asList("0", "1"),
                            Arrays.asList(
                                schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
                        .filter(
                            "3",
                            Arrays.asList(
                                timeSeries(schemaMap.get("root.sg.d1.s1")),
                                timeSeries(schemaMap.get("root.sg.d1.s2"))),
                            gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")),
                            false)
                        .getRoot(),
                    new TestPlanBuilder()
                        .fullOuterTimeJoin(
                            "6",
                            Ordering.ASC,
                            Arrays.asList("4", "5"),
                            Arrays.asList(
                                schemaMap.get("root.sg.d2.s1"), schemaMap.get("root.sg.d2.s2")))
                        .filter(
                            "7",
                            Arrays.asList(
                                timeSeries(schemaMap.get("root.sg.d2.s1")),
                                timeSeries(schemaMap.get("root.sg.d2.s2"))),
                            gt(timeSeries(schemaMap.get("root.sg.d2.s1")), intValue("10")),
                            false)
                        .getRoot(),
                    new TestPlanBuilder()
                        .scanAligned("8", schemaMap.get("root.sg.d2.a"))
                        .filter(
                            "9",
                            Arrays.asList(
                                timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                                timeSeries(schemaMap.get("root.sg.d2.a.s2"))),
                            gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")),
                            false)
                        .getRoot()))
            .getRoot(),
        new TestPlanBuilder()
            .deviceView(
                "10",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .leftOuterTimeJoin(
                            "11",
                            Ordering.ASC,
                            new TestPlanBuilder()
                                .scan(
                                    "0",
                                    schemaMap.get("root.sg.d1.s1"),
                                    gt(timeSeries(schemaMap.get("root.sg.d1.s1")), intValue("10")))
                                .getRoot(),
                            new TestPlanBuilder()
                                .scan("1", schemaMap.get("root.sg.d1.s2"))
                                .getRoot())
                        .project("12", Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"))
                        .getRoot(),
                    new TestPlanBuilder()
                        .leftOuterTimeJoin(
                            "13",
                            Ordering.ASC,
                            new TestPlanBuilder()
                                .scan(
                                    "4",
                                    schemaMap.get("root.sg.d2.s1"),
                                    gt(timeSeries(schemaMap.get("root.sg.d2.s1")), intValue("10")))
                                .getRoot(),
                            new TestPlanBuilder()
                                .scan("5", schemaMap.get("root.sg.d2.s2"))
                                .getRoot())
                        .project("14", Arrays.asList("root.sg.d2.s1", "root.sg.d2.s2"))
                        .getRoot(),
                    new TestPlanBuilder()
                        .scanAligned(
                            "8",
                            schemaMap.get("root.sg.d2.a"),
                            gt(timeSeries(schemaMap.get("root.sg.d2.a.s1")), intValue("10")))
                        .project("15", Arrays.asList("root.sg.d2.a.s1", "root.sg.d2.a.s2"))
                        .getRoot()))
            .getRoot());
  }

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    OptimizationTestUtil.checkPushDown(
        Collections.emptyList(), new PredicatePushDown(), sql, rawPlan, optPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    OptimizationTestUtil.checkCannotPushDown(
        Collections.emptyList(), new PredicatePushDown(), sql, rawPlan);
  }
}
