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
    AnalyzeVisitor visitor = new AnalyzeVisitor(partitionFetcher, schemaFetcher);
    Analysis analysis = null;
    context.setReserveMemoryForSchemaTreeFunc(
        mem -> {
          context.reserveMemoryForFrontEnd(mem);
          // For temporary and independently counted memory, we need process it immediately
          context.reserveMemoryForFrontEndImmediately();
        });
    try {
      analysis = visitor.process(statement, context);
    } finally {
      if (analysis != null && context.releaseSchemaTreeAfterAnalyzing()) {
        analysis.setSchemaTree(null);
        context.releaseMemoryForSchemaTree();
      }
      context.setReserveMemoryForSchemaTreeFunc(null);
    }
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
      QueryPlanCostMetricSet.getInstance().recordTreePlanCost(ANALYZER, analyzeCost);
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
