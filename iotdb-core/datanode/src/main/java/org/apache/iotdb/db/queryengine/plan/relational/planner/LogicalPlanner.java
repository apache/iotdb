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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CreateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.OptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class LogicalPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanner.class);
  private final MPPQueryContext queryContext;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator = new SymbolAllocator();
  private final List<PlanOptimizer> planOptimizers;
  private final Metadata metadata;
  private final WarningCollector warningCollector;

  public LogicalPlanner(
      MPPQueryContext queryContext,
      Metadata metadata,
      SessionInfo sessionInfo,
      WarningCollector warningCollector) {
    this.queryContext = queryContext;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    this.planOptimizers =
        new OptimizeFactory(new PlannerContext(metadata, new InternalTypeManager()))
            .getPlanOptimizers();
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    PlanNode planNode = planStatement(analysis, analysis.getStatement());

    if (analysis.getStatement() instanceof Query) {
      for (PlanOptimizer optimizer : planOptimizers) {
        planNode =
            optimizer.optimize(
                planNode,
                new PlanOptimizer.Context(
                    sessionInfo,
                    analysis,
                    metadata,
                    queryContext,
                    queryContext.getTypeProvider(),
                    symbolAllocator,
                    queryContext.getQueryId(),
                    warningCollector,
                    PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
      }
    }

    return new LogicalQueryPlan(queryContext, planNode);
  }

  private PlanNode planStatement(final Analysis analysis, final Statement statement) {
    if (statement instanceof CreateDevice) {
      return planCreateDevice((CreateDevice) statement, analysis);
    }
    if (statement instanceof FetchDevice) {
      return planFetchDevice((FetchDevice) statement, analysis);
    }
    if (statement instanceof ShowDevice) {
      return planShowDevice((ShowDevice) statement, analysis);
    }
    if (statement instanceof CountDevice) {
      return planCountDevice((CountDevice) statement, analysis);
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
    if (statement instanceof WrappedStatement) {
      return createRelationPlan(analysis, ((WrappedStatement) statement));
    }
    throw new IllegalStateException(
        "Unsupported statement type: " + statement.getClass().getSimpleName());
  }

  private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
    if (plan.getRoot() instanceof WritePlanNode) {
      return plan.getRoot();
    }
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

      columnHeaders.add(new ColumnHeader(name, getTSDataType(field.getType())));

      columnNumber++;
    }

    OutputNode outputNode =
        new OutputNode(
            queryContext.getQueryId().genPlanNodeId(),
            plan.getRoot(),
            names.build(),
            outputs.build());

    DatasetHeader respDatasetHeader = new DatasetHeader(columnHeaders, true);
    analysis.setRespDatasetHeader(respDatasetHeader);

    return outputNode;
  }

  private RelationPlan createRelationPlan(Analysis analysis, WrappedStatement statement) {
    return getRelationPlanner(analysis).process(statement, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(
        analysis, symbolAllocator, queryContext, sessionInfo, ImmutableMap.of());
  }

  private PlanNode planCreateDevice(CreateDevice statement, Analysis analysis) {
    queryContext.setQueryType(QueryType.WRITE);

    CreateTableDeviceNode node =
        new CreateTableDeviceNode(
            queryContext.getQueryId().genPlanNodeId(),
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
            queryContext.getSession().getUserName());
    analysis.setSchemaPartitionInfo(partition);

    return node;
  }

  private PlanNode planFetchDevice(FetchDevice statement, Analysis analysis) {
    queryContext.setQueryType(QueryType.READ);

    List<ColumnHeader> columnHeaderList =
        getColumnHeaderList(statement.getDatabase(), statement.getTableName());

    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaderList, true));

    TableDeviceFetchNode fetchNode =
        new TableDeviceFetchNode(
            queryContext.getQueryId().genPlanNodeId(),
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

  private PlanNode planShowDevice(final ShowDevice statement, final Analysis analysis) {
    final String database = planQueryDevice(statement, analysis);
    final List<ColumnHeader> columnHeaderList =
        getColumnHeaderList(database, statement.getTableName());
    analysis.setRespDatasetHeader(
        new DatasetHeader(getColumnHeaderList(database, statement.getTableName()), true));

    return new TableDeviceQueryScanNode(
        queryContext.getQueryId().genPlanNodeId(),
        database,
        statement.getTableName(),
        statement.getIdDeterminedPredicateList(),
        statement.getIdFuzzyPredicate(),
        columnHeaderList,
        null);
  }

  private PlanNode planCountDevice(final CountDevice statement, final Analysis analysis) {
    final String database = planQueryDevice(statement, analysis);
    final List<ColumnHeader> columnHeaderList =
        Collections.singletonList(new ColumnHeader("count(devices)", TSDataType.INT64));
    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaderList, true));

    final TableDeviceQueryCountNode node =
        new TableDeviceQueryCountNode(
            queryContext.getQueryId().genPlanNodeId(),
            database,
            statement.getTableName(),
            statement.getIdDeterminedPredicateList(),
            statement.getIdFuzzyPredicate(),
            columnHeaderList,
            null);

    final CountSchemaMergeNode countMergeNode =
        new CountSchemaMergeNode(queryContext.getQueryId().genPlanNodeId());
    countMergeNode.addChild(node);
    return countMergeNode;
  }

  private String planQueryDevice(final AbstractQueryDevice statement, final Analysis analysis) {
    queryContext.setQueryType(QueryType.READ);

    final String database =
        Objects.isNull(statement.getDatabase())
            ? analysis.getDatabaseName()
            : statement.getDatabase();

    if (Objects.isNull(database)) {
      analysis.setFinishQueryAfterAnalyze();
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.METADATA_ERROR.getStatusCode(),
              "The database must be set before show devices."));
    }

    final SchemaPartition schemaPartition =
        statement.isIdDetermined()
            ? metadata.getSchemaPartition(database, statement.getPartitionKeyList())
            : metadata.getSchemaPartition(database);
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }

    return database;
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
