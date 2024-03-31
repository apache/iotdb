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
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.MustBeClosed;
import io.airlift.log.Logger;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.cost.CachingTableStatsProvider;
import org.apache.iotdb.db.queryengine.plan.relational.cost.TableStatistics;
import org.apache.iotdb.db.queryengine.plan.relational.cost.TableStatsProvider;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableHandle;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.Statement;
import org.apache.iotdb.db.relational.sql.tree.Table;
import org.apache.iotdb.session.Session;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.forEachPair;
import static com.google.common.collect.Streams.zip;
import static java.util.Objects.requireNonNull;

public class LogicalPlanner
{
    private static final Logger LOG = Logger.get(LogicalPlanner.class);

    public enum Stage {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final MPPQueryContext context;

    private final Session session;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Metadata metadata;
    private final WarningCollector warningCollector;

    public LogicalPlanner(
            MPPQueryContext context,
            Metadata metadata,
            Session session,
            WarningCollector warningCollector) {
        this.context = context;
        this.metadata = metadata;
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public PlanNode plan(Analysis analysis) throws IoTDBException {
        return planStatement(analysis, analysis.getStatement());
    }

    public PlanNode planStatement(Analysis analysis, Statement statement) throws IoTDBException {
        return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
    }

    private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) throws IoTDBException {
        if (statement instanceof Query) {
            return createRelationPlan(analysis, (Query) statement);
        }
        throw new IoTDBException("Unsupported statement type " + statement.getClass().getSimpleName(), -1);
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        // TODO perfect the logic of outputDescriptor
        RelationType outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().orElse("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }

        return new OutputNode(context.getQueryId().genPlanNodeId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis, Query query) {
        return getRelationPlanner(analysis).process(query, null);
    }

    private RelationPlan createRelationPlan(Analysis analysis, Table table) {
        return getRelationPlanner(analysis).process(table, null);
    }

    private RelationPlanner getRelationPlanner(Analysis analysis) {
        return new RelationPlanner(analysis, symbolAllocator, context.getQueryId(), session, ImmutableMap.of());
    }
}
