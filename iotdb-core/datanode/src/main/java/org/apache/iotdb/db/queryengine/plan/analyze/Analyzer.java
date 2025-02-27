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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.ANALYZER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TREE_TYPE;

/** Analyze the statement and generate Analysis. */
public class Analyzer {
  private final MPPQueryContext context;

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  public Analyzer(
      MPPQueryContext context, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    this.context = context;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

  public Analysis analyze(Statement statement) {
    long startTime = System.nanoTime();
    Analysis analysis =
        new AnalyzeVisitor(partitionFetcher, schemaFetcher).process(statement, context);
    if (context.getSession() != null) {
      // for test compatibility
      analysis.setDatabaseName(context.getDatabaseName().orElse(null));
    }

    if (statement.isQuery()) {
      long analyzeCost =
          System.nanoTime()
              - startTime
              - context.getFetchSchemaCost()
              - context.getFetchPartitionCost();
      QueryPlanCostMetricSet.getInstance().recordPlanCost(TREE_TYPE, ANALYZER, analyzeCost);
      context.setAnalyzeCost(analyzeCost);
    }
    return analysis;
  }

  public static Analysis analyze(Statement statement, MPPQueryContext context) {
    return new Analyzer(
            context, ClusterPartitionFetcher.getInstance(), ClusterSchemaFetcher.getInstance())
        .analyze(statement);
  }
}
