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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ReplaceSymbolInExpression;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LiteralMarkerReplacer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.SqlFormatter;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLANNER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLAN_OPTIMIZE;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.SCHEMA_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TABLE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
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
    sb.append(analysis.getDatabaseName());
    sb.append(SqlFormatter.formatSql(statement));
    sb.append(queryContext.getZoneId());
    return sb.toString();
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

  private void adjustBySchema(PlanNode planNode, CachedValue cachedValue, Analysis analysis) {
    if (!(planNode instanceof DeviceTableScanNode)) {
      for (PlanNode child : planNode.getChildren()) {
        adjustBySchema(child, cachedValue, analysis);
      }
      return;
    }
    long startTime = System.nanoTime();
    DeviceTableScanNode deviceTableScanNode = (DeviceTableScanNode) planNode;
    final List<DeviceEntry> deviceEntries =
        metadata.indexScan(
            deviceTableScanNode.getQualifiedObjectName(),
            cachedValue.getMetadataExpressionList().stream()
                .map(
                    expression ->
                        ReplaceSymbolInExpression.transform(
                            expression, cachedValue.getAssignments()))
                .collect(Collectors.toList()),
            cachedValue.getAttributeColumns(),
            queryContext);
    deviceTableScanNode.setDeviceEntries(deviceEntries);

    final long schemaFetchCost = System.nanoTime() - startTime;
    QueryPlanCostMetricSet.getInstance()
        .recordPlanCost(TABLE_TYPE, SCHEMA_FETCHER, schemaFetchCost);
    queryContext.setFetchSchemaCost(schemaFetchCost);

    if (deviceEntries.isEmpty()) {
      if (analysis.noAggregates() && !analysis.hasJoinNode()) {
        // no device entries, queries(except aggregation and join) can be finished
        analysis.setEmptyDataSource(true);
        analysis.setFinishQueryAfterAnalyze();
      }
    } else {
      final Filter timeFilter =
          deviceTableScanNode
              .getTimePredicate()
              .map(value -> value.accept(new ConvertPredicateToTimeFilterVisitor(), null))
              .orElse(null);

      deviceTableScanNode.setTimeFilter(timeFilter);

      startTime = System.nanoTime();
      final DataPartition dataPartition =
          fetchDataPartitionByDevices(
              // for tree view, we need to pass actual tree db name to this method
              deviceTableScanNode instanceof TreeDeviceViewScanNode
                  ? ((TreeDeviceViewScanNode) deviceTableScanNode).getTreeDBName()
                  : deviceTableScanNode.getQualifiedObjectName().getDatabaseName(),
              deviceEntries,
              timeFilter);

      if (dataPartition.getDataPartitionMap().size() > 1) {
        throw new IllegalStateException(
            "Table model can only process data only in one database yet!");
      }

      if (dataPartition.getDataPartitionMap().isEmpty()) {
        if (analysis.noAggregates() && !analysis.hasJoinNode()) {
          // no data partitions, queries(except aggregation and join) can be finished
          analysis.setEmptyDataSource(true);
          analysis.setFinishQueryAfterAnalyze();
        }
      } else {
        analysis.upsertDataPartition(dataPartition);
      }

      final long fetchPartitionCost = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TABLE_TYPE, PARTITION_FETCHER, fetchPartitionCost);
      queryContext.setFetchPartitionCost(fetchPartitionCost);
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
        logger.info("CachedKey generated cost time: {}", curTime - totalStartTime);
        symbolAllocator.fill(cachedValue.getSymbolMap());
        analysis.setRespDatasetHeader(cachedValue.getRespHeader());
        adjustBySchema(cachedValue.planNode, cachedValue, analysis);

        for (int i = 0; i < cachedValue.getLiteralReference().size(); i++) {
          cachedValue.getLiteralReference().get(i).replace(literalList.get(i));
        }

        logger.info(
            "Logical plan is cached, adjustment cost time: {}", System.nanoTime() - curTime);
        logger.info("Logical plan is cached, cost time: {}", System.nanoTime() - totalStartTime);
        logger.info(
            "Logical plan is cached, fetch schema cost time: {}",
            queryContext.getFetchPartitionCost() + queryContext.getFetchSchemaCost());
        return new LogicalQueryPlan(queryContext, cachedValue.getPlanNode());
      }
      // Following implementation of plan should be based on the generalizedStatement
      literalReference = literalList;
    }

    PlanNode planNode = planStatement(analysis, statement);

    if (analysis.isQuery()) {
      long logicalPlanCostTime = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TABLE_TYPE, LOGICAL_PLANNER, logicalPlanCostTime);
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
      logger.info(
          "Logical plan is generated, optimization cost time: {}", System.nanoTime() - startTime);
      long logicalOptimizationCost =
          System.nanoTime()
              - startTime
              - queryContext.getFetchPartitionCost()
              - queryContext.getFetchSchemaCost();
      queryContext.setLogicalOptimizationCost(logicalOptimizationCost);
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TABLE_TYPE, LOGICAL_PLAN_OPTIMIZE, logicalOptimizationCost);

      PlanCacheManager.getInstance()
          .cacheValue(
              cachedKey,
              planNode,
              literalReference,
              analysis.getRespDatasetHeader(),
              symbolAllocator.cloneSymbolMap(),
              queryContext.getAssignments(),
              queryContext.getMetaDataExpressionList(),
              queryContext.getAttributeColumns());
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
      statement = ((PipeEnriched) statement).getInnerStatement();
      if (statement instanceof CreateOrUpdateDevice) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode) planCreateOrUpdateDevice((CreateOrUpdateDevice) statement, analysis));
      }
      if (statement instanceof Update) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode) planUpdate((Update) statement, analysis));
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
    if (queryContext.isExplainAnalyze()) {
      outputs.add(new Symbol(ColumnHeaderConstant.EXPLAIN_ANALYZE));
      names.add(ColumnHeaderConstant.EXPLAIN_ANALYZE);
      columnHeaders.add(new ColumnHeader(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT));
    } else {
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
        getDeviceColumnHeaderList(statement.getDatabase(), statement.getTableName());

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
            statement.getIdDeterminedFilterList(),
            null,
            statement.getColumnHeaderList(),
            null,
            Objects.isNull(statement.getIdFuzzyPredicate()) ? pushDownLimit : -1);

    // put the column type info into symbolAllocator to generate TypeProvider
    statement
        .getColumnHeaderList()
        .forEach(
            columnHeader ->
                symbolAllocator.newSymbol(
                    columnHeader.getColumnName(),
                    TypeFactory.getType(columnHeader.getColumnType())));

    // Filter
    if (Objects.nonNull(statement.getIdFuzzyPredicate())) {
      currentNode =
          new FilterNode(queryId.genPlanNodeId(), currentNode, statement.getIdFuzzyPredicate());
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
            statement.getIdDeterminedFilterList(),
            statement.getIdFuzzyPredicate(),
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
        statement.getIdDeterminedFilterList(),
        statement.getIdFuzzyPredicate(),
        statement.getColumnHeaderList(),
        null,
        statement.getAssignments(),
        queryContext.getSession());
  }

  private void planTraverseDevice(final AbstractTraverseDevice statement, final Analysis analysis) {
    final String database = statement.getDatabase();

    final SchemaPartition schemaPartition =
        statement.isIdDetermined()
            ? metadata.getSchemaPartition(database, statement.getPartitionKeyList())
            : metadata.getSchemaPartition(database);
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }
  }

  private RelationPlan planExplainAnalyze(final ExplainAnalyze statement, final Analysis analysis) {
    RelationPlan originalQueryPlan =
        createRelationPlan(analysis, (Query) (statement.getStatement()));
    Symbol symbol =
        symbolAllocator.newSymbol(ColumnHeaderConstant.EXPLAIN_ANALYZE, StringType.getInstance());
    PlanNode newRoot =
        new ExplainAnalyzeNode(
            queryContext.getQueryId().genPlanNodeId(),
            originalQueryPlan.getRoot(),
            statement.isVerbose(),
            queryContext.getLocalQueryId(),
            queryContext.getTimeOut(),
            symbol);
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
