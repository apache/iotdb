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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizationTestUtil {

  private OptimizationTestUtil() {
    // util class
  }

  public static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d1.s3", new MeasurementPath("root.sg.d1.s3", TSDataType.BOOLEAN));
      schemaMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d2.s4", new MeasurementPath("root.sg.d2.s4", TSDataType.TEXT));

      MeasurementPath aS1 = new MeasurementPath("root.sg.d2.a.s1", TSDataType.INT32);
      aS1.setUnderAlignedEntity(true);
      MeasurementPath aS2 = new MeasurementPath("root.sg.d2.a.s2", TSDataType.DOUBLE);
      aS2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s1", aS1);
      schemaMap.put("root.sg.d2.a.s2", aS2);

      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s1", "s2"),
              Arrays.asList(aS1.getMeasurementSchema(), aS2.getMeasurementSchema()));
      AlignedPath descOrderAlignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s2", "s1"),
              Arrays.asList(aS2.getMeasurementSchema(), aS1.getMeasurementSchema()));
      schemaMap.put("root.sg.d2.a", alignedPath);
      schemaMap.put("desc_root.sg.d2.a", descOrderAlignedPath);

      AlignedPath aligned_d2s1 =
          new AlignedPath(
              "root.sg.d2.a",
              Collections.singletonList("s1"),
              Collections.singletonList(aS1.getMeasurementSchema()));
      schemaMap.put("aligned_root.sg.d2.a.s1", aligned_d2s1);
      AlignedPath aligned_d2s2 =
          new AlignedPath(
              "root.sg.d2.a",
              Collections.singletonList("s2"),
              Collections.singletonList(aS2.getMeasurementSchema()));
      schemaMap.put("aligned_root.sg.d2.a.s2", aligned_d2s2);

      // used in GROUP BY LEVEL
      schemaMap.put("root.sg.*.s1", new MeasurementPath("root.sg.*.s1", TSDataType.INT32));
      schemaMap.put("root.sg.*.s2", new MeasurementPath("root.sg.*.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.*.*.s1", new MeasurementPath("root.sg.*.*.s1", TSDataType.INT32));
      schemaMap.put("root.sg.*.*.s2", new MeasurementPath("root.sg.*.*.s2", TSDataType.DOUBLE));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  public static void checkPushDown(
      List<PlanOptimizer> preOptimizers,
      PlanOptimizer optimizer,
      String sql,
      PlanNode rawPlan,
      PlanNode optPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    PlanNode actualPlan =
        new LogicalPlanVisitor(analysis).process(analysis.getTreeStatement(), context);
    for (PlanOptimizer preOptimizer : preOptimizers) {
      actualPlan = preOptimizer.optimize(actualPlan, analysis, context);
    }
    Assert.assertEquals(rawPlan, actualPlan);

    PlanNode actualOptPlan = optimizer.optimize(actualPlan, analysis, context);
    actualOptPlan =
        new OrderByExpressionWithLimitChangeToTopK().optimize(actualOptPlan, analysis, context);
    Assert.assertEquals(optPlan, actualOptPlan);
  }

  public static void checkCannotPushDown(
      List<PlanOptimizer> preOptimizers, PlanOptimizer optimizer, String sql, PlanNode rawPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    PlanNode actualPlan =
        new LogicalPlanVisitor(analysis).process(analysis.getTreeStatement(), context);
    for (PlanOptimizer preOptimizer : preOptimizers) {
      actualPlan = preOptimizer.optimize(actualPlan, analysis, context);
    }
    Assert.assertEquals(rawPlan, actualPlan);

    PlanNode actualOptPlan = optimizer.optimize(actualPlan, analysis, context);
    Assert.assertEquals(actualPlan, actualOptPlan);
  }

  public static AggregationDescriptor getAggregationDescriptor(AggregationStep step, String path) {
    return new AggregationDescriptor(
        TAggregationType.COUNT.name().toLowerCase(),
        step,
        Collections.singletonList(new TimeSeriesOperand(schemaMap.get(path))),
        new HashMap<>());
  }
}
