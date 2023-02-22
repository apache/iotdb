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
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.mpp.plan.optimization.PlanFactory.limit;
import static org.apache.iotdb.db.mpp.plan.optimization.PlanFactory.scan;

public class LimitOffsetPushDownTest {

  private static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));

      MeasurementPath d2s1 = new MeasurementPath("root.sg.d2.s1", TSDataType.INT32);
      MeasurementPath d2s2 = new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE);
      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2",
              Arrays.asList("s1", "s2"),
              Arrays.asList(d2s1.getMeasurementSchema(), d2s2.getMeasurementSchema()));
      d2s1.setUnderAlignedEntity(true);
      d2s2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.s1", d2s1);
      schemaMap.put("root.sg.d2.s2", d2s2);
      schemaMap.put("root.sg.d2", alignedPath);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  private static final MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));

  @Test
  public void test() {
    String sql = "select s1 from root.sg.d1 limit 100";
    Analysis analysis = getAnalysis(sql);
    PlanNode actualPlan = getPlan(analysis);

    PlanNode rawPlan = limit("1", 100, scan("0", schemaMap.get("root.sg.d1.s1")));
    PlanNode optPlan = scan("0", schemaMap.get("root.sg.d1.s1"), 100, 0);

    Assert.assertEquals(rawPlan, actualPlan);
    Assert.assertEquals(optPlan, new LimitOffsetPushDown().optimize(actualPlan, analysis, context));
  }

  private Analysis getAnalysis(String sql) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    return analyzer.analyze(statement);
  }

  private PlanNode getPlan(Analysis analysis) {
    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    return planner.plan(analysis).getRootNode();
  }
}
