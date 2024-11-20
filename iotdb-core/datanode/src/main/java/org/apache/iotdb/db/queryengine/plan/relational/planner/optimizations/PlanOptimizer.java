/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;

import static java.util.Objects.requireNonNull;

public interface PlanOptimizer {
  PlanNode optimize(PlanNode plan, Context context);

  class Context {
    private final SessionInfo sessionInfo;
    private final Analysis analysis;
    private final Metadata metadata;
    private final MPPQueryContext queryContext;
    private final SymbolAllocator symbolAllocator;
    private final QueryId queryIdAllocator;
    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

    public Context(
        SessionInfo sessionInfo,
        Analysis analysis,
        Metadata metadata,
        MPPQueryContext queryContext,
        SymbolAllocator symbolAllocator,
        QueryId queryIdAllocator,
        WarningCollector warningCollector,
        PlanOptimizersStatsCollector planOptimizersStatsCollector) {
      this.sessionInfo = sessionInfo;
      this.analysis = analysis;
      this.metadata = metadata;
      this.queryContext = queryContext;
      this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
      this.queryIdAllocator = requireNonNull(queryIdAllocator, "idAllocator is null");
      this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
      this.planOptimizersStatsCollector =
          requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
    }

    public SessionInfo sessionInfo() {
      return sessionInfo;
    }

    public Analysis getAnalysis() {
      return analysis;
    }

    public Metadata getMetadata() {
      return metadata;
    }

    public MPPQueryContext getQueryContext() {
      return queryContext;
    }

    public SymbolAllocator getSymbolAllocator() {
      return symbolAllocator;
    }

    public QueryId idAllocator() {
      return queryIdAllocator;
    }

    public WarningCollector warningCollector() {
      return warningCollector;
    }

    public PlanOptimizersStatsCollector planOptimizersStatsCollector() {
      return planOptimizersStatsCollector;
    }
  }
}
