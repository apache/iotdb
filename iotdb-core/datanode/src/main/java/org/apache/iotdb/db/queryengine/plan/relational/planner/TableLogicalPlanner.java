/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ReplaceSymbolInExpression;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.LogicalOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDeviceWithCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LiteralMarkerReplacer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableNameRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.SqlFormatter;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLANNER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLAN_OPTIMIZE;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.SCHEMA_FETCHER;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.createQualifiedObjectName;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.CachedValue.cloneMetadataExpressions;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.CachedValue.clonePlanWithNewLiterals;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.CachedValue.collectDeviceTableScanNodes;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.visibleFields;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode.COLUMN_NAME_PREFIX;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice.COUNT_DEVICE_HEADER_STRING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice.getDeviceColumnHeaderList;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableLogicalPlanner {
  private final MPPQueryContext queryContext;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator;
  private final List<PlanOptimizer> planOptimizers;
  private final Metadata metadata;
  private final WarningCollector warningCollector;

  @TestOnly
  public TableLogicalPlanner(
      MPPQueryContext queryContext,
      Metadata metadata,
      SessionInfo sessionInfo,
      SymbolAllocator symbolAllocator,
      WarningCollector warningCollector) {
    this(
        queryContext,
        metadata,
        sessionInfo,
        symbolAllocator,
        warningCollector,
        new LogicalOptimizeFactory(new PlannerContext(metadata, new InternalTypeManager()))
            .getPlanOptimizers());
  }

  public TableLogicalPlanner(
      MPPQueryContext queryContext,
      Metadata metadata,
      SessionInfo sessionInfo,
      SymbolAllocator symbolAllocator,
      WarningCollector warningCollector,
      List<PlanOptimizer> planOptimizers) {
    this.queryContext = queryContext;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    this.planOptimizers = planOptimizers;
  }

  private List<Literal> generalizeStatement(Query query) {
    LiteralMarkerReplacer literalMarkerReplacer = new LiteralMarkerReplacer();
    literalMarkerReplacer.process(query);
    return literalMarkerReplacer.getLiteralList();
  }

  private String calculateCacheKey(Statement statement, Analysis analysis) {
    StringBuilder sb = new StringBuilder();
    Statement normalized = TableNameRewriter.rewrite(statement, analysis.getDatabaseName());
    sb.append(SqlFormatter.formatSql(normalized));
    long version = DataNodeTableCache.getInstance().getVersion();
    sb.append(version);
    String rawKey = sb.toString();
    String md5Key = DigestUtils.md5Hex(rawKey);
    return md5Key;
  }

  private static final Logger logger = LoggerFactory.getLogger(TableLogicalPlanner.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private DataPartition fetchDataPartitionByDevices(
      final String
          database, // for tree view, database should be the real tree db name with `root.` prefix
      final List<DeviceEntry> deviceEntries,
      final Filter globalTimeFilter) {
    final Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(globalTimeFilter, queryContext);

    // there is no satisfied time range
    if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
      return new DataPartition(
          Collections.emptyMap(),
          CONFIG.getSeriesPartitionExecutorClass(),
          CONFIG.getSeriesPartitionSlotNum());
    }

    final List<DataPartitionQueryParam> dataPartitionQueryParams =
        deviceEntries.stream()
            .map(
                deviceEntry ->
                    new DataPartitionQueryParam(
                        deviceEntry.getDeviceID(), res.left, res.right.left, res.right.right))
            .collect(Collectors.toList());

    if (res.right.left || res.right.right) {
      return metadata.getDataPartitionWithUnclosedTimeRange(database, dataPartitionQueryParams);
    } else {
      return metadata.getDataPartition(database, dataPartitionQueryParams);
    }
  }

  /** The metadataExpression in cachedValue needs to be used to update the scanNode in planNode */
  private void adjustSchema(
      DeviceTableScanNode scanNode,
      List<Expression> metaExprs,
      List<String> attributeCols,
      Map<Symbol, ColumnSchema> assignments,
      Analysis analysis) {

    long startTime = System.nanoTime();

    Map<String, List<DeviceEntry>> deviceEntriesMap =
        metadata.indexScan(
            scanNode.getQualifiedObjectName(), metaExprs, attributeCols, queryContext);

    String deviceDatabase =
        !deviceEntriesMap.isEmpty() ? deviceEntriesMap.keySet().iterator().next() : null;
    List<DeviceEntry> deviceEntries =
        deviceDatabase != null ? deviceEntriesMap.get(deviceDatabase) : Collections.emptyList();
    scanNode.setDeviceEntries(deviceEntries);

    Filter timeFilter =
        scanNode
            .getTimePredicate()
            .map(v -> v.accept(new ConvertPredicateToTimeFilterVisitor(), null))
            .orElse(null);
    scanNode.setTimeFilter(timeFilter);

    DataPartition dataPartition =
        fetchDataPartitionByDevices(
            scanNode instanceof TreeDeviceViewScanNode
                ? ((TreeDeviceViewScanNode) scanNode).getTreeDBName()
                : scanNode.getQualifiedObjectName().getDatabaseName(),
            deviceEntries,
            timeFilter);
    analysis.upsertDataPartition(dataPartition);

    long schemaFetchCost = System.nanoTime() - startTime;
    QueryPlanCostMetricSet.getInstance().recordTablePlanCost(SCHEMA_FETCHER, schemaFetchCost);
    queryContext.setFetchSchemaCost(schemaFetchCost);

    if (deviceEntries.isEmpty() && analysis.noAggregates() && !analysis.hasJoinNode()) {
      analysis.setEmptyDataSource(true);
      analysis.setFinishQueryAfterAnalyze();
    }
  }

  boolean enableCache = true;

  public LogicalQueryPlan plan(final Analysis analysis) {
    long startTime = System.nanoTime();
    long totalStartTime = startTime;
    Statement statement = analysis.getStatement();

    // Try to use plan cache
    // We should check if statement gis Query in enablePlanCache() method\
    String cachedKey = "";

    List<Literal> literalReference = null;

    if (enableCache && statement instanceof Query) {
      List<Literal> literalList = generalizeStatement((Query) statement);
      cachedKey = calculateCacheKey(statement, analysis);
      CachedValue cachedValue = PlanCacheManager.getInstance().getCachedValue(cachedKey);
      if (cachedValue != null) {
        // deal with the device stuff
        long curTime = System.nanoTime();
        // logger.info("CachedKey generated cost time: {}", curTime - totalStartTime);
        symbolAllocator.fill(cachedValue.getSymbolMap());
        symbolAllocator.setNextId(cachedValue.getSymbolNextId());
        analysis.setRespDatasetHeader(cachedValue.getRespHeader());

        // Clone the PlanNode with new literals
        CachedValue.ClonerContext clonerContext =
            new CachedValue.ClonerContext(queryContext.getQueryId(), literalList);
        PlanNode newPlan = clonePlanWithNewLiterals(cachedValue.getPlanNode(), clonerContext);
        // Clone the metadata expressions with new literals
        List<List<Expression>> newMetadataExpressionLists = new ArrayList<>();
        if (cachedValue.getMetadataExpressionLists() != null) {
          for (List<Expression> exprList : cachedValue.getMetadataExpressionLists()) {
            if (exprList != null) {
              newMetadataExpressionLists.add(cloneMetadataExpressions(exprList, literalList));
            } else {
              // occupy an empty list and maintain a one-to-one correspondence with scanNodes
              newMetadataExpressionLists.add(new ArrayList<>());
            }
          }
        }

        List<DeviceTableScanNode> scanNodes = collectDeviceTableScanNodes(newPlan);

        for (int i = 0; i < scanNodes.size(); i++) {
          DeviceTableScanNode scanNode = scanNodes.get(i);

          List<Expression> metaExprs =
              i < newMetadataExpressionLists.size()
                  ? newMetadataExpressionLists.get(i)
                  : Collections.emptyList();
          List<String> attributeCols =
              i < cachedValue.getAttributeColumnsLists().size()
                  ? cachedValue.getAttributeColumnsLists().get(i)
                  : Collections.emptyList();
          Map<Symbol, ColumnSchema> assignments =
              i < cachedValue.getAssignmentsLists().size()
                  ? cachedValue.getAssignmentsLists().get(i)
                  : Collections.emptyMap();

          adjustSchema(scanNode, metaExprs, attributeCols, assignments, analysis);
        }

        logger.info(
            "Logical plan is cached, adjustment cost time: {}", System.nanoTime() - curTime);
        logger.info("Logical plan is cached, cost time: {}", System.nanoTime() - totalStartTime);
        logger.info(
            "Logical plan is cached, fetch schema cost time: {}",
            queryContext.getFetchPartitionCost() + queryContext.getFetchSchemaCost());
        return new LogicalQueryPlan(queryContext, newPlan);
      }
      // Following implementation of plan should be based on the generalizedStatement
      literalReference = literalList;
    }

    // The logical plan was not hit. The logical plan generation stage needs to be executed
    PlanNode planNode = planStatement(analysis, statement);

    if (analysis.isQuery()) {
      long logicalPlanCostTime = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordTablePlanCost(LOGICAL_PLANNER, logicalPlanCostTime);
      queryContext.setLogicalPlanCost(logicalPlanCostTime);

      startTime = System.nanoTime();
      for (PlanOptimizer optimizer : planOptimizers) {
        planNode =
            optimizer.optimize(
                planNode,
                new PlanOptimizer.Context(
                    sessionInfo,
                    analysis,
                    metadata,
                    queryContext,
                    symbolAllocator,
                    queryContext.getQueryId(),
                    warningCollector,
                    PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
      }
      long logicalOptimizationCost =
          System.nanoTime()
              - startTime
              - queryContext.getFetchPartitionCost()
              - queryContext.getFetchSchemaCost();
      queryContext.setLogicalOptimizationCost(logicalOptimizationCost);
      QueryPlanCostMetricSet.getInstance()
          .recordTablePlanCost(LOGICAL_PLAN_OPTIMIZE, logicalOptimizationCost);

      CachedValue.ClonerContext clonerContext =
          new CachedValue.ClonerContext(queryContext.getQueryId(), literalReference);
      PlanNode clonedPlan = clonePlanWithNewLiterals(planNode, clonerContext);
      List<DeviceTableScanNode> scanNodes = collectDeviceTableScanNodes(clonedPlan);

      PlanCacheManager.getInstance()
          .cacheValue(
              cachedKey,
              clonedPlan,
              scanNodes,
              literalReference,
              analysis.getRespDatasetHeader(),
              symbolAllocator.cloneSymbolMap(),
              symbolAllocator.getNextId(),
              queryContext.getMetadataExpressionLists(),
              queryContext.getAttributeColumnsLists(),
              queryContext.getAssignmentsLists());
    }
    logger.info(
        "Logical plan is generated, fetch schema cost time: {}",
        queryContext.getFetchPartitionCost() + queryContext.getFetchSchemaCost());
    logger.info("Logical plan is generated, cost time: {}", System.nanoTime() - totalStartTime);

    return new LogicalQueryPlan(queryContext, planNode);
  }

  private PlanNode planStatement(final Analysis analysis, Statement statement) {
    // Schema statements are handled here
    if (statement instanceof PipeEnriched) {
      Statement innerStatement = ((PipeEnriched) statement).getInnerStatement();
      if (innerStatement instanceof CreateOrUpdateDevice) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode)
                planCreateOrUpdateDevice((CreateOrUpdateDevice) innerStatement, analysis));
      }
      if (innerStatement instanceof Update) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode) planUpdate((Update) innerStatement, analysis));
      }
    }
    if (statement instanceof CreateOrUpdateDevice) {
      return planCreateOrUpdateDevice((CreateOrUpdateDevice) statement, analysis);
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
    if (statement instanceof Update) {
      return planUpdate((Update) statement, analysis);
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
    if (statement instanceof LoadTsFile) {
      return createRelationPlan(analysis, (LoadTsFile) statement);
    }
    if (statement instanceof PipeEnriched) {
      return createRelationPlan(analysis, (PipeEnriched) statement);
    }
    if (statement instanceof Delete) {
      return createRelationPlan(analysis, (Delete) statement);
    }
    if (statement instanceof ExplainAnalyze) {
      return planExplainAnalyze((ExplainAnalyze) statement, analysis);
    }
    if (statement instanceof Insert) {
      return genInsertPlan(analysis, (Insert) statement);
    }
    throw new IllegalStateException(
        "Unsupported statement type: " + statement.getClass().getSimpleName());
  }

  private RelationPlan genInsertPlan(final Analysis analysis, final Insert node) {
    // query plan and visible fields
    Query query = node.getQuery();
    RelationPlan plan = createRelationPlan(analysis, query);
    List<Symbol> visibleFieldMappings = visibleFields(plan);

    // table columns
    Table table = node.getTable();
    QualifiedObjectName targetTable = createQualifiedObjectName(sessionInfo, table.getName());
    Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionInfo, targetTable);
    if (!tableSchema.isPresent()) {
      TableMetadataImpl.throwTableNotExistsException(
          targetTable.getDatabaseName(), targetTable.getObjectName());
    }

    // insert columns
    Analysis.Insert insert = analysis.getInsert();
    List<ColumnSchema> insertColumns = insert.getColumns();

    Assignments.Builder assignments = Assignments.builder();
    List<Symbol> neededInputColumnNames = new ArrayList<>(insertColumns.size());

    for (int i = 0, size = insertColumns.size(); i < size; i++) {
      Symbol output =
          symbolAllocator.newSymbol(insertColumns.get(i).getName(), insertColumns.get(i).getType());
      Symbol input = visibleFieldMappings.get(i);
      neededInputColumnNames.add(output);
      assignments.put(output, input.toSymbolReference());
    }

    // Project Node
    ProjectNode projectNode =
        new ProjectNode(
            queryContext.getQueryId().genPlanNodeId(), plan.getRoot(), assignments.build());
    List<Field> fields =
        insertColumns.stream()
            .map(
                column ->
                    Field.newUnqualified(
                        column.getName(), column.getType(), column.getColumnCategory()))
            .collect(toImmutableList());
    Scope scope =
        Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();
    plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols(), Optional.empty());

    // Into Node
    IntoNode intoNode =
        new IntoNode(
            queryContext.getQueryId().genPlanNodeId(),
            plan.getRoot(),
            targetTable.getDatabaseName(),
            table.getName().getSuffix(),
            insertColumns,
            neededInputColumnNames,
            symbolAllocator.newSymbol(Insert.ROWS, Insert.ROWS_TYPE));
    return new RelationPlan(
        intoNode, analysis.getRootScope(), intoNode.getOutputSymbols(), Optional.empty());
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
    if (queryContext.isExplainAnalyze()) {
      outputs.add(new Symbol(ColumnHeaderConstant.EXPLAIN_ANALYZE));
      names.add(ColumnHeaderConstant.EXPLAIN_ANALYZE);
      columnHeaders.add(new ColumnHeader(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT));
    } else {
      RelationType outputDescriptor = analysis.getOutputDescriptor();
      for (Field field : outputDescriptor.getVisibleFields()) {
        String name = field.getName().orElse(null);

        if (name == null) {
          String originColumnName = field.getOriginColumnName().orElse(null);
          StringBuilder stringBuilder = new StringBuilder(COLUMN_NAME_PREFIX).append(columnNumber);
          // process of expr with Column, we record originColumnName in Field when analyze
          if (originColumnName != null) {
            stringBuilder.append('_').append(originColumnName);
          }
          name = stringBuilder.toString();
        }

        names.add(name);
        int fieldIndex = outputDescriptor.indexOf(field);
        Symbol symbol = plan.getSymbol(fieldIndex);
        outputs.add(symbol);

        columnHeaders.add(new ColumnHeader(name, getTSDataType(field.getType())));

        columnNumber++;
      }
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

  private RelationPlan createRelationPlan(Analysis analysis, LoadTsFile loadTsFile) {
    return getRelationPlanner(analysis).process(loadTsFile, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, PipeEnriched pipeEnriched) {
    return getRelationPlanner(analysis).process(pipeEnriched, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Delete statement) {
    return getRelationPlanner(analysis).process(statement, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(
        analysis, symbolAllocator, queryContext, Optional.empty(), sessionInfo, ImmutableMap.of());
  }

  private PlanNode planCreateOrUpdateDevice(
      final CreateOrUpdateDevice statement, final Analysis analysis) {
    final CreateOrUpdateTableDeviceNode node =
        new CreateOrUpdateTableDeviceNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTable(),
            statement.getDeviceIdList(),
            statement.getAttributeNameList(),
            statement.getAttributeValueList());

    analysis.setStatement(statement);
    final SchemaPartition partition =
        metadata.getOrCreateSchemaPartition(
            statement.getDatabase(),
            node.getPartitionKeyList(),
            queryContext.getSession().getUserName());
    analysis.setSchemaPartitionInfo(partition);

    return node;
  }

  private PlanNode planFetchDevice(final FetchDevice statement, final Analysis analysis) {
    final List<ColumnHeader> columnHeaderList =
        getDeviceColumnHeaderList(statement.getDatabase(), statement.getTableName(), null);

    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaderList, true));

    final TableDeviceFetchNode fetchNode =
        new TableDeviceFetchNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getDeviceIdList(),
            statement.getPartitionKeyList(),
            columnHeaderList,
            null);

    final SchemaPartition schemaPartition =
        metadata.getSchemaPartition(statement.getDatabase(), statement.getPartitionKeyList());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }

    return fetchNode;
  }

  private PlanNode planShowDevice(final ShowDevice statement, final Analysis analysis) {
    planQueryDevice(statement, analysis);

    final QueryId queryId = queryContext.getQueryId();

    long pushDownLimit =
        Objects.nonNull(statement.getLimit())
            ? analysis.getLimit(statement.getLimit()).orElse(-1)
            : -1;
    if (pushDownLimit > -1 && Objects.nonNull(statement.getOffset())) {
      pushDownLimit += analysis.getOffset(statement.getOffset());
    }

    // Scan
    PlanNode currentNode =
        new TableDeviceQueryScanNode(
            queryId.genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getTagDeterminedFilterList(),
            null,
            statement.getColumnHeaderList(),
            null,
            Objects.isNull(statement.getTagFuzzyPredicate()) ? pushDownLimit : -1,
            statement.needAligned());

    // put the column type info into symbolAllocator to generate TypeProvider
    statement
        .getColumnHeaderList()
        .forEach(
            columnHeader ->
                symbolAllocator.newSymbol(
                    columnHeader.getColumnName(),
                    TypeFactory.getType(columnHeader.getColumnType())));

    // Filter
    if (Objects.nonNull(statement.getTagFuzzyPredicate())) {
      currentNode =
          new FilterNode(queryId.genPlanNodeId(), currentNode, statement.getTagFuzzyPredicate());
    }

    // Limit
    if (pushDownLimit > -1) {
      currentNode =
          new LimitNode(queryId.genPlanNodeId(), currentNode, pushDownLimit, Optional.empty());
    }

    // Offset
    return Objects.nonNull(statement.getOffset())
        ? new OffsetNode(
            queryId.genPlanNodeId(), currentNode, analysis.getOffset(statement.getOffset()))
        : currentNode;
  }

  private PlanNode planCountDevice(final CountDevice statement, final Analysis analysis) {
    planQueryDevice(statement, analysis);

    final TableDeviceQueryCountNode node =
        new TableDeviceQueryCountNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getTagDeterminedFilterList(),
            statement.getTagFuzzyPredicate(),
            statement.getColumnHeaderList());

    // put the column type info into symbolAllocator to generate TypeProvider
    statement
        .getColumnHeaderList()
        .forEach(
            columnHeader ->
                symbolAllocator.newSymbol(
                    columnHeader.getColumnName(),
                    TypeFactory.getType(columnHeader.getColumnType())));
    symbolAllocator.newSymbol(COUNT_DEVICE_HEADER_STRING, LongType.INT64);
    final CountSchemaMergeNode countMergeNode =
        new CountSchemaMergeNode(queryContext.getQueryId().genPlanNodeId());
    countMergeNode.addChild(node);
    return countMergeNode;
  }

  private void planQueryDevice(
      final AbstractQueryDeviceWithCache statement, final Analysis analysis) {
    planTraverseDevice(statement, analysis);

    if (!analysis.isFailed()) {
      analysis.setRespDatasetHeader(statement.getDataSetHeader());
    }
  }

  private PlanNode planUpdate(final Update statement, final Analysis analysis) {
    planTraverseDevice(statement, analysis);

    return new TableDeviceAttributeUpdateNode(
        queryContext.getQueryId().genPlanNodeId(),
        statement.getDatabase(),
        statement.getTableName(),
        statement.getTagDeterminedFilterList(),
        statement.getTagFuzzyPredicate(),
        statement.getColumnHeaderList(),
        null,
        statement.getAssignments(),
        queryContext.getSession());
  }

  private void planTraverseDevice(final AbstractTraverseDevice statement, final Analysis analysis) {
    final String database = statement.getDatabase();

    final TsTable table =
        DataNodeTableCache.getInstance().getTable(database, statement.getTableName());

    if (TreeViewSchema.isTreeViewTable(table)) {
      final PathPatternTree tree = new PathPatternTree();
      tree.appendPathPattern(TreeViewSchema.getPrefixPattern(table));
      tree.constructTree();

      analysis.setSchemaPartitionInfo(
          ClusterPartitionFetcher.getInstance().getSchemaPartition(tree));

      if (analysis.getSchemaPartitionInfo().getSchemaPartitionMap().size() > 1) {
        throw new SemanticException(
            "Tree device view with multiple databases("
                + analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet()
                + ") is unsupported yet.");
      }
    } else {
      analysis.setSchemaPartitionInfo(
          statement.isIdDetermined()
              ? metadata.getSchemaPartition(database, statement.getPartitionKeyList())
              : metadata.getSchemaPartition(database));
    }

    if (analysis.getSchemaPartitionInfo().isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }
  }

  private RelationPlan planExplainAnalyze(final ExplainAnalyze statement, final Analysis analysis) {
    RelationPlan originalQueryPlan =
        createRelationPlan(analysis, (Query) (statement.getStatement()));
    Symbol symbol =
        symbolAllocator.newSymbol(ColumnHeaderConstant.EXPLAIN_ANALYZE, StringType.getInstance());

    // recording permittedOutputs of ExplainAnalyzeNode's child
    RelationType outputDescriptor = analysis.getOutputDescriptor(statement.getStatement());
    ImmutableList.Builder<Symbol> childPermittedOutputs = ImmutableList.builder();
    for (Field field : outputDescriptor.getVisibleFields()) {
      int fieldIndex = outputDescriptor.indexOf(field);
      Symbol columnSymbol = originalQueryPlan.getSymbol(fieldIndex);
      childPermittedOutputs.add(columnSymbol);
    }

    PlanNode newRoot =
        new ExplainAnalyzeNode(
            queryContext.getQueryId().genPlanNodeId(),
            originalQueryPlan.getRoot(),
            statement.isVerbose(),
            queryContext.getLocalQueryId(),
            queryContext.getTimeOut(),
            symbol,
            childPermittedOutputs.build());
    return new RelationPlan(
        newRoot,
        originalQueryPlan.getScope(),
        originalQueryPlan.getFieldMappings(),
        Optional.empty());
  }

  private enum Stage {
    CREATED,
    OPTIMIZED,
    OPTIMIZED_AND_VALIDATED
  }
}
