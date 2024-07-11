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

import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceQueryNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.IterativeOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.RuleStatsRecorder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFilterColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneLimitColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOffsetColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOutputSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneProjectColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneSortColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CreateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.RemoveRedundantIdentityProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SimplifyExpressions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.TablePlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class LogicalPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanner.class);
  private final MPPQueryContext context;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator = new SymbolAllocator();
  private final List<PlanOptimizer> planOptimizers;
  // TODO Remove after all rules are modified
  private final List<TablePlanOptimizer> tablePlanOptimizers;
  private final Metadata metadata;
  private final WarningCollector warningCollector;

  public LogicalPlanner(
      MPPQueryContext context,
      Metadata metadata,
      SessionInfo sessionInfo,
      WarningCollector warningCollector) {
    this.context = context;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    Set<Rule<?>> rules =
        ImmutableSet.of(
            new PruneFilterColumns(),
            new PruneLimitColumns(),
            new PruneOffsetColumns(),
            new PruneOutputSourceColumns(),
            new PruneProjectColumns(),
            new PruneSortColumns(),
            new PruneTableScanColumns(metadata));
    this.planOptimizers =
        ImmutableList.of(
            new IterativeOptimizer(
                new PlannerContext(metadata, new InternalTypeManager()),
                new RuleStatsRecorder(),
                rules));
    this.tablePlanOptimizers =
        Arrays.asList(
            new SimplifyExpressions(),
            // new PruneUnUsedColumns(),
            new RemoveRedundantIdentityProjections(),
            new PushPredicateIntoTableScan());
  }

  @TestOnly
  public LogicalPlanner(
      MPPQueryContext context,
      Metadata metadata,
      SessionInfo sessionInfo,
      List<TablePlanOptimizer> tablePlanOptimizers,
      WarningCollector warningCollector) {
    this.context = context;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    this.planOptimizers = ImmutableList.of();
    this.tablePlanOptimizers = tablePlanOptimizers;
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    PlanNode planNode = planStatement(analysis, analysis.getStatement());

    // TODO remove after all optimizer rewritten as Trino-like
    for (TablePlanOptimizer optimizer : tablePlanOptimizers) {
      planNode = optimizer.optimize(planNode, analysis, metadata, sessionInfo, context);
    }

    for (PlanOptimizer optimizer : planOptimizers) {
      planNode =
          optimizer.optimize(
              planNode,
              new PlanOptimizer.Context(
                  sessionInfo,
                  context.getTypeProvider(),
                  symbolAllocator,
                  context.getQueryId(),
                  warningCollector,
                  PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
    }

    // TODO remove after introduce InlineProjections and RemoveRedundantIdentityProjections
    planNode =
        tablePlanOptimizers.get(1).optimize(planNode, analysis, metadata, sessionInfo, context);
    return new LogicalQueryPlan(context, planNode);
  }

  private PlanNode planStatement(Analysis analysis, Statement statement) {
    if (statement instanceof CreateDevice) {
      return planCreateDevice((CreateDevice) statement, analysis);
    }
    if (statement instanceof FetchDevice) {
      return planFetchDevice((FetchDevice) statement, analysis);
    }
    if (statement instanceof ShowDevice) {
      return planShowDevice((ShowDevice) statement, analysis);
    }
    return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
  }

  private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) {
    if (statement instanceof Query) {
      return createRelationPlan(analysis, (Query) statement);
    }
    if (statement instanceof Explain) {
      return createRelationPlan(analysis, (Query) ((Explain) statement).getStatement());
    }
    throw new IllegalStateException(
        "Unsupported statement type: " + statement.getClass().getSimpleName());
  }

  private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    List<ColumnHeader> columnHeaders = new ArrayList<>();

    int columnNumber = 0;
    // TODO perfect the logic of outputDescriptor
    RelationType outputDescriptor = analysis.getOutputDescriptor();
    for (Field field : outputDescriptor.getVisibleFields()) {
      String name = field.getName().orElse("_col" + columnNumber);

      names.add(name);
      int fieldIndex = outputDescriptor.indexOf(field);
      Symbol symbol = plan.getSymbol(fieldIndex);
      outputs.add(symbol);

      if (!TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(name)) {
        columnHeaders.add(new ColumnHeader(symbol.getName(), getTSDataType(field.getType())));
      }

      columnNumber++;
    }

    OutputNode outputNode =
        new OutputNode(
            context.getQueryId().genPlanNodeId(), plan.getRoot(), names.build(), outputs.build());

    DatasetHeader respDatasetHeader = new DatasetHeader(columnHeaders, false);
    analysis.setRespDatasetHeader(respDatasetHeader);

    return outputNode;
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(analysis, symbolAllocator, context, sessionInfo, ImmutableMap.of());
  }

  private PlanNode planCreateDevice(CreateDevice statement, Analysis analysis) {
    context.setQueryType(QueryType.WRITE);

    CreateTableDeviceNode node =
        new CreateTableDeviceNode(
            context.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTable(),
            statement.getDeviceIdList(),
            statement.getAttributeNameList(),
            statement.getAttributeValueList());

    analysis.setStatement(statement);
    SchemaPartition partition =
        metadata.getOrCreateSchemaPartition(
            statement.getDatabase(),
            node.getPartitionKeyList(),
            context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(partition);

    return node;
  }

  private PlanNode planFetchDevice(FetchDevice statement, Analysis analysis) {
    context.setQueryType(QueryType.READ);

    List<ColumnHeader> columnHeaderList =
        getColumnHeaderList(statement.getDatabase(), statement.getTableName());

    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaderList, true));

    TableDeviceFetchNode fetchNode =
        new TableDeviceFetchNode(
            context.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getDeviceIdList(),
            columnHeaderList,
            null);

    SchemaPartition schemaPartition =
        metadata.getSchemaPartition(statement.getDatabase(), statement.getPartitionKeyList());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }

    return fetchNode;
  }

  private PlanNode planShowDevice(ShowDevice statement, Analysis analysis) {
    context.setQueryType(QueryType.READ);

    List<ColumnHeader> columnHeaderList =
        getColumnHeaderList(statement.getDatabase(), statement.getTableName());

    TableDeviceQueryNode queryNode =
        new TableDeviceQueryNode(
            context.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getIdDeterminedPredicateList(),
            statement.getIdFuzzyPredicate(),
            columnHeaderList,
            null);

    SchemaPartition schemaPartition =
        statement.isIdDetermined()
            ? metadata.getSchemaPartition(statement.getDatabase(), statement.getPartitionKeyList())
            : metadata.getSchemaPartition(statement.getDatabase());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }

    return queryNode;
  }

  private List<ColumnHeader> getColumnHeaderList(String database, String tableName) {
    List<TsTableColumnSchema> columnSchemaList =
        DataNodeTableCache.getInstance().getTable(database, tableName).getColumnList();

    List<ColumnHeader> columnHeaderList = new ArrayList<>(columnSchemaList.size());
    for (TsTableColumnSchema columnSchema : columnSchemaList) {
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)
          || columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        columnHeaderList.add(
            new ColumnHeader(columnSchema.getColumnName(), columnSchema.getDataType()));
      }
    }
    return columnHeaderList;
  }

  private enum Stage {
    CREATED,
    OPTIMIZED,
    OPTIMIZED_AND_VALIDATED
  }
}
