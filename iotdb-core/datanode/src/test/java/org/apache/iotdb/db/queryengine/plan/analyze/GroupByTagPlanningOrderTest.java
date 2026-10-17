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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GroupByTagPlanningOrderTest {

  @Test
  public void testAnalysisPreservesGroupEncounterOrder() throws Exception {
    Analysis analysis = analyze(createQueryContext());
    assertEquals(
        Arrays.asList(Collections.singletonList("v1"), Collections.singletonList("v0")),
        new ArrayList<>(analysis.getTagValuesToGroupedTimeseriesOperands().keySet()));
  }

  @Test
  public void testLogicalPlanPreservesAnalyzedGroupOrder() throws Exception {
    MPPQueryContext context = createQueryContext();
    Analysis analysis = analyze(context);
    Map<List<String>, LinkedHashMap<Expression, List<Expression>>> groups = new LinkedHashMap<>();
    for (String value : Arrays.asList("v1", "v0")) {
      List<String> tagValues = Collections.singletonList(value);
      groups.put(tagValues, analysis.getTagValuesToGroupedTimeseriesOperands().get(tagValues));
    }

    GroupByTagNode node =
        (GroupByTagNode)
            new LogicalPlanBuilder(analysis, context)
                .planRawDataSource(analysis.getSourceExpressions(), Ordering.ASC, 0, 0, false)
                .planGroupByTag(
                    analysis.getCrossGroupByExpressions(),
                    analysis.getTagKeys(),
                    groups,
                    null,
                    Ordering.ASC)
                .getRoot();
    assertEquals(
        new ArrayList<>(groups.keySet()),
        new ArrayList<>(node.getTagValuesToAggregationDescriptors().keySet()));
  }

  private Analysis analyze(MPPQueryContext context) throws Exception {
    ClusterSchemaTree testSchemaTree = new ClusterSchemaTree();
    testSchemaTree.setDatabases(Collections.singleton("root.sg"));
    for (int i = 1; i <= 2; i++) {
      testSchemaTree.appendSingleMeasurement(
          new PartialPath("root.sg.d" + i + ".s1"),
          new MeasurementSchema("s1", TSDataType.INT32),
          Collections.singletonMap("key1", i == 1 ? "v1" : "v0"),
          null,
          null,
          false);
    }
    FakeSchemaFetcherImpl schemaFetcher =
        new FakeSchemaFetcherImpl() {
          @Override
          public ClusterSchemaTree fetchSchemaWithTags(
              PathPatternTree patternTree,
              boolean withTemplate,
              MPPQueryContext queryContext,
              boolean canSeeAuditDB) {
            return testSchemaTree;
          }
        };
    return new Analyzer(context, new FakePartitionFetcherImpl(), schemaFetcher)
        .analyze(
            StatementGenerator.createStatement(
                "select count(s1) from root.sg.* group by tags(key1)",
                ZonedDateTime.now().getOffset()));
  }

  private MPPQueryContext createQueryContext() {
    return new MPPQueryContext(
        "",
        new QueryId("test_query"),
        new SessionInfo(0, "root", ZoneId.systemDefault()),
        new TEndPoint(),
        new TEndPoint());
  }
}
