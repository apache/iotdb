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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ParallelizeGroupingTest {

  @Test
  public void testExternalTsFileScanKeepsGroupNodeWhenPartitionedByAllTags() {
    Symbol region = new Symbol("region");
    Symbol site = new Symbol("site");
    Map<Symbol, ColumnSchema> assignments =
        ImmutableMap.of(
            region, new ColumnSchema("region", STRING, false, TAG),
            site, new ColumnSchema("site", STRING, false, TAG));

    MPPQueryContext queryContext = new MPPQueryContext(new QueryId("parallelize_grouping"));
    ExternalTsFileQueryResource resource =
        new ExternalTsFileQueryResource(
            queryContext,
            Path.of("target", "parallelize-grouping"),
            "metrics",
            Collections.emptyList(),
            assignments,
            1024 * 1024);
    try {
      ExternalTsFileScanNode scanNode =
          new ExternalTsFileScanNode(
              new PlanNodeId("scan"),
              QualifiedObjectName.valueOf("external.metrics"),
              ImmutableList.of(region, site),
              assignments,
              ImmutableMap.of(region, 0, site, 1),
              resource);
      OrderingScheme orderingScheme =
          new OrderingScheme(
              ImmutableList.of(region, site),
              ImmutableMap.of(region, ASC_NULLS_LAST, site, ASC_NULLS_LAST));
      GroupNode groupNode = new GroupNode(new PlanNodeId("group"), scanNode, orderingScheme, 2);

      Analysis analysis = new Analysis(null, Collections.emptyMap());
      analysis.setQuery(true);
      PlanNode optimizedPlan =
          new ParallelizeGrouping()
              .optimize(
                  groupNode,
                  new PlanOptimizer.Context(
                      SESSION_INFO,
                      analysis,
                      TEST_MATADATA,
                      queryContext,
                      new SymbolAllocator(),
                      new QueryId("parallelize_grouping"),
                      WarningCollector.NOOP,
                      PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));

      assertTrue(optimizedPlan instanceof GroupNode);
      assertSame(scanNode, ((GroupNode) optimizedPlan).getChild());
    } finally {
      resource.closeByQueryExecution();
    }
  }
}
