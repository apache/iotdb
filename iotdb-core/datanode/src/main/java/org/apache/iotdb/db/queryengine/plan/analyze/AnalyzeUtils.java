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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression.Operator;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeResp;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TimeRange;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.And;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.DeviceIn;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.SegmentExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.SegmentNotNull;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;

public class AnalyzeUtils {

  private static final int ATTRIBUTE_FILTER_DELETE_DEVICE_IN_LIMIT = 1000;
  private static final CommonQueryAstVisitor<PredicateParseResult, PredicateParseContext>
      DELETION_PREDICATE_PARSE_VISITOR =
          new CommonQueryAstVisitor<>() {
            @Override
            public PredicateParseResult visitLogicalExpression(
                final LogicalExpression node, final PredicateParseContext context) {
              parseAndPredicate(node, context.expressionQueue);
              return PredicateParseResult.empty(context.tagPredicate);
            }

            @Override
            public PredicateParseResult visitComparisonExpression(
                final ComparisonExpression node, final PredicateParseContext context) {
              return parseComparison(node, context.timeRange, context.tagPredicate, context.table);
            }

            @Override
            public PredicateParseResult visitIsNullPredicate(
                final IsNullPredicate node, final PredicateParseContext context) {
              return parseIsNull(node, context.tagPredicate, context.table);
            }

            @Override
            public PredicateParseResult visitIsNotNullPredicate(
                final IsNotNullPredicate node, final PredicateParseContext context) {
              return parseIsNotNull(node, context.tagPredicate, context.table);
            }

            @Override
            public PredicateParseResult visitLikePredicate(
                final LikePredicate node, final PredicateParseContext context) {
              return parseLike(node, context.tagPredicate, context.table);
            }

            @Override
            public PredicateParseResult visitInPredicate(
                final InPredicate node, final PredicateParseContext context) {
              return parseIn(node, context.tagPredicate, context.table);
            }

            @Override
            public PredicateParseResult visitExpression(
                final Expression node, final PredicateParseContext context) {
              throw new SemanticException(
                  DataNodeQueryMessages.UNSUPPORTED_EXPRESSION
                      + node
                      + " in "
                      + context.rootExpression);
            }
          };

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeUtils.class);

  private AnalyzeUtils() {
    // util class
  }

  public static InsertBaseStatement analyzeInsert(
      MPPQueryContext context,
      InsertBaseStatement insertBaseStatement,
      Runnable schemaValidation,
      DataPartitionQueryFunc partitionFetcher,
      DataPartitionQueryParamComputation partitionQueryParamComputation,
      IAnalysis analysis,
      boolean removeLogicalView) {
    context.setQueryType(QueryType.WRITE);
    insertBaseStatement.semanticCheck();
    validateSchema(analysis, insertBaseStatement, schemaValidation);

    InsertBaseStatement realStatement =
        removeLogicalView ? removeLogicalView(analysis, insertBaseStatement) : insertBaseStatement;
    if (analysis.isFinishQueryAfterAnalyze()) {
      return realStatement;
    }
    analysis.setRealStatement(realStatement);

    analyzeDataPartition(
        analysis,
        partitionQueryParamComputation.compute(realStatement, context),
        context.getSession().getUserName(),
        partitionFetcher);
    return realStatement;
  }

  public static String getDatabaseName(
      final InsertBaseStatement statement, final MPPQueryContext context) {
    if (statement.getDatabaseName().isPresent()) {
      return statement.getDatabaseName().get();
    }
    if (context != null && context.getDatabaseName().isPresent()) {
      return context.getDatabaseName().get();
    }
    return null;
  }

  public static List<DataPartitionQueryParam> computeTableDataPartitionParams(
      final InsertBaseStatement statement, final MPPQueryContext context) {
    if (statement instanceof InsertTabletStatement) {
      final InsertTabletStatement insertTabletStatement = (InsertTabletStatement) statement;
      final Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
        timePartitionSlotMap
            .computeIfAbsent(insertTabletStatement.getTableDeviceID(i), id -> new HashSet<>())
            .add(insertTabletStatement.getTimePartitionSlot(i));
      }
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
    } else if (statement instanceof InsertMultiTabletsStatement) {
      final InsertMultiTabletsStatement insertMultiTabletsStatement =
          (InsertMultiTabletsStatement) statement;
      final Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (final InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
          timePartitionSlotMap
              .computeIfAbsent(insertTabletStatement.getTableDeviceID(i), id -> new HashSet<>())
              .add(insertTabletStatement.getTimePartitionSlot(i));
        }
      }
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
    } else if (statement instanceof InsertRowStatement) {
      final InsertRowStatement insertRowStatement = (InsertRowStatement) statement;
      return computeDataPartitionParams(
          Collections.singletonMap(
              insertRowStatement.getTableDeviceID(),
              Collections.singleton(insertRowStatement.getTimePartitionSlot())),
          getDatabaseName(statement, context));
    } else if (statement instanceof InsertRowsStatement) {
      final InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      final Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (final InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        timePartitionSlotMap
            .computeIfAbsent(insertRowStatement.getTableDeviceID(), id -> new HashSet<>())
            .add(insertRowStatement.getTimePartitionSlot());
      }
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
    }
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.COMPUTEDATAPARTITIONPARAMS_FOR + statement);
  }

  public static List<DataPartitionQueryParam> computeTreeDataPartitionParams(
      InsertBaseStatement statement, MPPQueryContext context) {
    if (statement instanceof InsertTabletStatement) {
      DataPartitionQueryParam dataPartitionQueryParam =
          getTreeDataPartitionQueryParam((InsertTabletStatement) statement, context);
      return Collections.singletonList(dataPartitionQueryParam);
    } else if (statement instanceof InsertMultiTabletsStatement) {
      InsertMultiTabletsStatement insertMultiTabletsStatement =
          (InsertMultiTabletsStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> dataPartitionQueryParamMap = new HashMap<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        Set<TTimePartitionSlot> timePartitionSlotSet =
            dataPartitionQueryParamMap.computeIfAbsent(
                insertTabletStatement.getDevicePath().getIDeviceIDAsFullDevice(),
                k -> new HashSet<>());
        timePartitionSlotSet.addAll(insertTabletStatement.getTimePartitionSlots());
      }
      return computeDataPartitionParams(
          dataPartitionQueryParamMap, getDatabaseName(statement, context));
    } else if (statement instanceof InsertRowsStatement) {
      final InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> dataPartitionQueryParamMap = new HashMap<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        Set<TTimePartitionSlot> timePartitionSlotSet =
            dataPartitionQueryParamMap.computeIfAbsent(
                insertRowStatement.getDevicePath().getIDeviceIDAsFullDevice(),
                k -> new HashSet<>());
        timePartitionSlotSet.add(insertRowStatement.getTimePartitionSlot());
      }
      return computeDataPartitionParams(
          dataPartitionQueryParamMap, getDatabaseName(statement, context));
    }
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.COMPUTEDATAPARTITIONPARAMS_FOR + statement);
  }

  private static DataPartitionQueryParam getTreeDataPartitionQueryParam(
      InsertTabletStatement statement, MPPQueryContext context) {
    DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDeviceID(statement.getDevicePath().getIDeviceIDAsFullDevice());
    dataPartitionQueryParam.setTimePartitionSlotList(statement.getTimePartitionSlots());
    dataPartitionQueryParam.setDatabaseName(getDatabaseName(statement, context));
    return dataPartitionQueryParam;
  }

  /**
   * @param dataPartitionQueryParamMap IDeviceID's first segment should be tableName without
   *     databaseName.
   * @param databaseName must start with root.
   */
  public static List<DataPartitionQueryParam> computeDataPartitionParams(
      Map<IDeviceID, Set<TTimePartitionSlot>> dataPartitionQueryParamMap, String databaseName) {
    List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
    for (Map.Entry<IDeviceID, Set<TTimePartitionSlot>> entry :
        dataPartitionQueryParamMap.entrySet()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDeviceID(entry.getKey());
      dataPartitionQueryParam.setTimePartitionSlotList(new ArrayList<>(entry.getValue()));
      dataPartitionQueryParam.setDatabaseName(databaseName);
      dataPartitionQueryParams.add(dataPartitionQueryParam);
    }
    return dataPartitionQueryParams;
  }

  public static void validateSchema(
      IAnalysis analysis, InsertBaseStatement insertStatement, Runnable schemaValidation) {
    final long startTime = System.nanoTime();
    try {
      schemaValidation.run();
    } catch (SemanticException e) {
      analysis.setFinishQueryAfterAnalyze(true);
      if (e.getCause() instanceof IoTDBException) {
        IoTDBException exception = (IoTDBException) e.getCause();
        analysis.setFailStatus(
            RpcUtils.getStatus(exception.getErrorCode(), exception.getMessage()));
      } else {
        if (e.getErrorCode() != TSStatusCode.SEMANTIC_ERROR.getStatusCode()) {
          // a specific code has been set, use it
          analysis.setFailStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        } else {
          // use METADATA_ERROR by default
          analysis.setFailStatus(
              RpcUtils.getStatus(TSStatusCode.METADATA_ERROR.getStatusCode(), e.getMessage()));
        }
      }
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleSchemaValidateCost(System.nanoTime() - startTime);
    }
    boolean hasFailedMeasurement = insertStatement.hasFailedMeasurements();
    String partialInsertMessage;
    if (hasFailedMeasurement) {
      partialInsertMessage =
          String.format(
              "Fail to insert measurements %s caused by %s",
              insertStatement.getFailedMeasurements(), insertStatement.getFailedMessages());
      LOGGER.warn(partialInsertMessage);
      analysis.setFailStatus(
          RpcUtils.getStatus(TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
    }
  }

  public static InsertBaseStatement removeLogicalView(
      IAnalysis analysis, InsertBaseStatement insertBaseStatement) {
    try {
      return insertBaseStatement.removeLogicalView();
    } catch (SemanticException e) {
      analysis.setFinishQueryAfterAnalyze(true);
      if (e.getCause() instanceof IoTDBException) {
        IoTDBException exception = (IoTDBException) e.getCause();
        analysis.setFailStatus(
            RpcUtils.getStatus(exception.getErrorCode(), exception.getMessage()));
      } else {
        analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage()));
      }
      return insertBaseStatement;
    }
  }

  /** get analysis according to statement and params */
  public static void analyzeDataPartition(
      final IAnalysis analysis,
      final List<DataPartitionQueryParam> dataPartitionQueryParams,
      final String userName,
      final DataPartitionQueryFunc partitionQueryFunc) {

    final DataPartition dataPartition =
        partitionQueryFunc.queryDataPartition(dataPartitionQueryParams, userName);
    if (dataPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(),
              "Database not exists and failed to create automatically "
                  + "because enable_auto_create_schema is FALSE."));
    }
    analysis.setDataPartitionInfo(dataPartition);
  }

  public static void analyzeDelete(final Delete node, final MPPQueryContext queryContext) {
    queryContext.setQueryType(QueryType.OTHER);
    validateSchema(node, queryContext);

    try (final ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      node.setReplicaSets(fetchDeleteReplicaSets(configNodeClient, node));
    } catch (final IoTDBRuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.CAN_NOT_CONNECT_CONFIGNODE.getStatusCode());
    }
  }

  static Set<TRegionReplicaSet> fetchDeleteReplicaSets(
      final ConfigNodeClient configNodeClient, final Delete node) throws Exception {
    final Set<TRegionReplicaSet> replicaSets = new HashSet<>();
    for (final TableDeletionEntry tableDeletionEntry : node.getTableDeletionEntries()) {
      final TGetRegionGroupsByTimeResp resp =
          configNodeClient.getRegionGroupsByTime(
              new TGetRegionGroupsByTimeReq(
                  node.getDatabaseName(),
                  tableDeletionEntry.getStartTime(),
                  tableDeletionEntry.getEndTime()));
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBRuntimeException(resp.getStatus());
      }
      if (resp.isSetRegionReplicaSets()) {
        replicaSets.addAll(resp.getRegionReplicaSets());
      }
    }
    return replicaSets;
  }

  @SuppressWarnings("java:S3655") // optional is checked
  public static String getDatabaseName(final Delete node, final MPPQueryContext queryContext) {
    final String databaseName;
    if (node.getTable().getName().getPrefix().isPresent()) {
      databaseName = node.getTable().getName().getPrefix().get().toString();
    } else if (queryContext.getDatabaseName().isPresent()) {
      databaseName = queryContext.getDatabaseName().get();
    } else {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    return databaseName;
  }

  private static void validateSchema(final Delete node, final MPPQueryContext queryContext) {
    final String tableName = node.getTable().getName().getSuffix();
    final String databaseName = getDatabaseName(node, queryContext);
    node.setDatabaseName(databaseName);

    final TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName);

    DataNodeTreeViewSchemaUtils.checkTableInWrite(databaseName, table);
    // Maybe set by pipe transfer
    if (Objects.isNull(node.getTableDeletionEntries())) {
      node.setTableDeletionEntries(
          parseExpressions2ModEntries(
              node.getWhere().orElse(null), table, databaseName, queryContext));
    }
  }

  public static List<TableDeletionEntry> parseExpressions2ModEntries(
      final Expression expression,
      final TsTable table,
      final String databaseName,
      final MPPQueryContext queryContext) {
    return toDisjunctiveNormalForms(expression).stream()
        .map(
            disjunctiveNormalForm ->
                parsePredicate(disjunctiveNormalForm, table, databaseName, queryContext))
        .collect(Collectors.toList());
  }

  /**
   * Convert to a disjunctive normal forms.
   *
   * <p>For example: ( A | B ) & ( C | D ) => ( A & C ) | ( A & D ) | ( B & C ) | ( B & D)
   *
   * <p>Returns the original expression if the expression is null or if the distribution will expand
   * the expression by too much.
   */
  public static List<Expression> toDisjunctiveNormalForms(Expression expression) {
    if (!(expression instanceof LogicalExpression)) {
      return Collections.singletonList(expression);
    }

    LogicalExpression logicalExpression = (LogicalExpression) expression;
    if (logicalExpression.getOperator() == Operator.AND) {
      // ( A | B ) & ( C | D ) => ( A & C ) | ( A & D ) | ( B & C ) | ( B & D)
      List<Expression> results = null;
      for (Expression term : logicalExpression.getTerms()) {
        if (results == null) {
          results = toDisjunctiveNormalForms(term);
        } else {
          results =
              crossProductOfDisjunctiveNormalForms(
                  results, toDisjunctiveNormalForms(term), Operator.AND);
        }
      }
      return results;
    } else if (logicalExpression.getOperator() == Operator.OR) {
      // ( A | B ) | ( C | D ) => A | B | C | D
      List<Expression> results = new ArrayList<>();
      for (Expression term : logicalExpression.getTerms()) {
        results.addAll(toDisjunctiveNormalForms(term));
      }
      return results;
    } else {
      throw new SemanticException(
          DataNodeQueryMessages.UNSUPPORTED_OPERATOR + logicalExpression.getOperator());
    }
  }

  private static List<Expression> crossProductOfDisjunctiveNormalForms(
      List<Expression> leftList, List<Expression> rightList, Operator operator) {
    List<Expression> results = new ArrayList<>();
    for (Expression leftExp : leftList) {
      for (Expression rightExp : rightList) {
        List<Expression> terms = new ArrayList<>();
        if (leftExp instanceof LogicalExpression) {
          terms.addAll(((LogicalExpression) leftExp).getTerms());
        } else {
          terms.add(leftExp);
        }

        if (rightExp instanceof LogicalExpression) {
          terms.addAll(((LogicalExpression) rightExp).getTerms());
        } else {
          terms.add(rightExp);
        }

        results.add(new LogicalExpression(operator, terms));
      }
    }
    return results;
  }

  private static TableDeletionEntry parsePredicate(
      Expression expression,
      TsTable table,
      final String databaseName,
      final MPPQueryContext queryContext) {
    if (expression == null) {
      return new TableDeletionEntry(
          new DeletionPredicate(table.getTableName()),
          new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE, true).toTsFileTimeRange());
    }

    final PredicateParseContext predicateParseContext =
        new PredicateParseContext(table, new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE, true));
    predicateParseContext.expressionQueue.add(expression);
    DeletionPredicate predicate = new DeletionPredicate(table.getTableName());
    predicateParseContext.rootExpression = expression;
    while (!predicateParseContext.expressionQueue.isEmpty()) {
      final Expression currExp = predicateParseContext.expressionQueue.remove();
      applyPredicateParseResult(
          currExp,
          DELETION_PREDICATE_PARSE_VISITOR.process(currExp, predicateParseContext),
          predicateParseContext);
    }
    if (Objects.nonNull(predicateParseContext.attributeColumns)) {
      final Set<IDeviceID> deviceIDs =
          TableDeviceSchemaFetcher.getInstance()
              .fetchDeviceSchemaForDataQuery(
                  databaseName,
                  table.getTableName(),
                  predicateParseContext.deviceFilterExpressions,
                  predicateParseContext.attributeColumns,
                  queryContext)
              .values()
              .stream()
              .flatMap(List::stream)
              .map(DeviceEntry::getDeviceID)
              .collect(Collectors.toSet());
      if (deviceIDs.size() > ATTRIBUTE_FILTER_DELETE_DEVICE_IN_LIMIT) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.TOO_MANY_DEVICES_MATCHED_BY_ATTRIBUTE_FILTERS_IN_DELETION,
                deviceIDs.size(),
                ATTRIBUTE_FILTER_DELETE_DEVICE_IN_LIMIT,
                predicateParseContext.attributeColumns));
      }
      predicateParseContext.tagPredicate = new DeviceIn(deviceIDs);
    }
    if (predicateParseContext.tagPredicate != null) {
      predicate.setTagPredicate(predicateParseContext.tagPredicate);
    }
    if (predicateParseContext.timeRange.getStartTime()
        > predicateParseContext.timeRange.getEndTime()) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.START_TIME_IS_GREATER_THAN_END_TIME,
              predicateParseContext.timeRange.getStartTime(),
              predicateParseContext.timeRange.getEndTime()));
    }

    return new TableDeletionEntry(predicate, predicateParseContext.timeRange.toTsFileTimeRange());
  }

  private static void applyPredicateParseResult(
      final Expression expression,
      final PredicateParseResult parseResult,
      final PredicateParseContext context) {
    context.tagPredicate = parseResult.tagPredicate;
    if (parseResult.shouldQueryDevice()) {
      if (Objects.isNull(context.attributeColumns)) {
        context.attributeColumns = new ArrayList<>();
      }
      addDeviceFilterExpression(expression, context);
      collectAttributeColumn(context.attributeColumns, parseResult.attributeColumn);
    } else if (parseResult.shouldFilterDevice()) {
      addDeviceFilterExpression(expression, context);
    }
  }

  private static void addDeviceFilterExpression(
      final Expression expression, final PredicateParseContext context) {
    if (Objects.isNull(context.deviceFilterExpressions)) {
      context.deviceFilterExpressions = new ArrayList<>();
    }
    context.deviceFilterExpressions.add(toSymbolReferenceExpression(expression));
  }

  private static void parseAndPredicate(
      LogicalExpression expression, Queue<Expression> expressionQueue) {
    if (expression.getOperator() != Operator.AND) {
      throw new SemanticException(DataNodeQueryMessages.ONLY_SUPPORT_AND_OPERATOR_IN_DELETION);
    }
    expressionQueue.addAll(expression.getTerms());
  }

  private static void collectAttributeColumn(
      final List<String> attributeColumns, final String attributeColumn) {
    if (Objects.nonNull(attributeColumn) && !attributeColumns.contains(attributeColumn)) {
      attributeColumns.add(attributeColumn);
    }
  }

  private static PredicateParseResult parseIsNull(
      IsNullPredicate isNullPredicate, TagPredicate oldPredicate, TsTable table) {
    Expression leftHandExp = isNullPredicate.getValue();
    if (!(leftHandExp instanceof Identifier)) {
      throw new SemanticException(
          DataNodeQueryMessages.LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER + leftHandExp);
    }
    String columnName = ((Identifier) leftHandExp).getValue();
    final TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
    if (Objects.nonNull(columnSchema)
        && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
      return PredicateParseResult.attribute(columnName, oldPredicate);
    }
    int tagColumnOrdinal = table.getTagColumnOrdinal(columnName);
    if (tagColumnOrdinal == -1) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_COLUMN_S_DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN,
              columnName));
    }

    // the first segment is the table name, so + 1
    TagPredicate newPredicate = new SegmentExactMatch(null, tagColumnOrdinal + 1);
    return PredicateParseResult.tag(combinePredicates(oldPredicate, newPredicate));
  }

  private static PredicateParseResult parseIsNotNull(
      IsNotNullPredicate isNotNullPredicate, TagPredicate oldPredicate, TsTable table) {
    Expression leftHandExp = isNotNullPredicate.getValue();
    if (!(leftHandExp instanceof Identifier)) {
      throw new SemanticException(
          DataNodeQueryMessages.LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER + leftHandExp);
    }
    String columnName = ((Identifier) leftHandExp).getValue();
    final TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
    if (Objects.nonNull(columnSchema)
        && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
      return PredicateParseResult.attribute(columnName, oldPredicate);
    }
    int tagColumnOrdinal = table.getTagColumnOrdinal(columnName);
    if (tagColumnOrdinal == -1) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_COLUMN_S_DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN,
              columnName));
    }

    // the first segment is the table name, so + 1
    TagPredicate newPredicate = new SegmentNotNull(tagColumnOrdinal + 1);
    return PredicateParseResult.tag(combinePredicates(oldPredicate, newPredicate));
  }

  private static PredicateParseResult parseLike(
      LikePredicate likePredicate, TagPredicate oldPredicate, TsTable table) {
    Expression leftHandExp = likePredicate.getValue();
    if (!(leftHandExp instanceof Identifier)) {
      throw new SemanticException(
          DataNodeQueryMessages.LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER + leftHandExp);
    }
    String columnName = ((Identifier) leftHandExp).getValue();
    final TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
    if (Objects.nonNull(columnSchema)
        && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
      validateAttributeComparison(likePredicate);
      return PredicateParseResult.attribute(columnName, oldPredicate);
    }
    int tagColumnOrdinal = table.getTagColumnOrdinal(columnName);
    if (tagColumnOrdinal == -1) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_COLUMN_S_DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN,
              columnName));
    }
    throw new SemanticException(
        DataNodeQueryMessages.THE_OPERATOR_OF_TAG_PREDICATE_MUST_BE_FOR
            + likePredicate.getPattern());
  }

  private static PredicateParseResult parseIn(
      InPredicate inPredicate, TagPredicate oldPredicate, TsTable table) {
    Expression leftHandExp = inPredicate.getValue();
    if (!(leftHandExp instanceof Identifier)) {
      throw new SemanticException(
          DataNodeQueryMessages.LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER + leftHandExp);
    }
    String columnName = ((Identifier) leftHandExp).getValue();
    final TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
    if (Objects.nonNull(columnSchema)
        && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
      validateAttributeComparison(inPredicate);
      return PredicateParseResult.attribute(columnName, oldPredicate);
    }
    int tagColumnOrdinal = table.getTagColumnOrdinal(columnName);
    if (tagColumnOrdinal == -1) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_COLUMN_S_DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN,
              columnName));
    }
    throw new SemanticException(
        DataNodeQueryMessages.THE_OPERATOR_OF_TAG_PREDICATE_MUST_BE_FOR
            + inPredicate.getValueList());
  }

  private static TagPredicate combinePredicates(
      TagPredicate oldPredicate, TagPredicate newPredicate) {
    if (oldPredicate == null) {
      return newPredicate;
    }
    if (oldPredicate instanceof TagPredicate.And) {
      ((And) oldPredicate).add(newPredicate);
      return oldPredicate;
    }
    return new TagPredicate.And(oldPredicate, newPredicate);
  }

  private static PredicateParseResult parseComparison(
      ComparisonExpression comparisonExpression,
      TimeRange timeRange,
      TagPredicate oldPredicate,
      TsTable table) {
    Expression left = comparisonExpression.getLeft();
    Expression right = comparisonExpression.getRight();
    if (!(left instanceof Identifier)) {
      throw new SemanticException(
          DataNodeQueryMessages.THE_LEFT_HAND_VALUE_MUST_BE_AN_IDENTIFIER + left);
    }
    Identifier identifier = (Identifier) left;
    // time predicate
    if (identifier.getValue().equalsIgnoreCase(getTimeColumnName(table))) {
      long rightHandValue;
      if (right instanceof LongLiteral) {
        rightHandValue = ((LongLiteral) right).getParsedValue();
      } else {
        throw new SemanticException(
            DataNodeQueryMessages.THE_RIGHT_HAND_VALUE_OF_TIME_PREDICATE_MUST_BE_A_LONG + right);
      }

      switch (comparisonExpression.getOperator()) {
        case LESS_THAN:
          if (rightHandValue == Long.MIN_VALUE) {
            throw new SemanticException(
                "The time predicate does not select any time range: " + comparisonExpression);
          }
          timeRange.setEndTime(Math.min(timeRange.getEndTime(), rightHandValue - 1));
          break;
        case LESS_THAN_OR_EQUAL:
          timeRange.setEndTime(Math.min(timeRange.getEndTime(), rightHandValue));
          break;
        case GREATER_THAN:
          if (rightHandValue == Long.MAX_VALUE) {
            throw new SemanticException(
                "The time predicate does not select any time range: " + comparisonExpression);
          }
          timeRange.setStartTime(Math.max(timeRange.getStartTime(), rightHandValue + 1));
          break;
        case GREATER_THAN_OR_EQUAL:
          timeRange.setStartTime(Math.max(timeRange.getStartTime(), rightHandValue));
          break;
        case EQUAL:
          timeRange.setStartTime(rightHandValue);
          timeRange.setEndTime(rightHandValue);
          break;
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
        default:
          throw new SemanticException(
              DataNodeQueryMessages.THE_OPERATOR_OF_TIME_PREDICATE_MUST_BE_FOR + right);
      }

      return PredicateParseResult.time(oldPredicate);
    }
    // tag predicate
    String columnName = identifier.getValue();
    final TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
    if (Objects.nonNull(columnSchema)
        && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
      validateAttributeComparison(comparisonExpression);
      return PredicateParseResult.attribute(columnName, oldPredicate);
    }
    int tagColumnOrdinal = table.getTagColumnOrdinal(columnName);
    if (tagColumnOrdinal == -1) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_COLUMN_S_DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN,
              columnName));
    }

    TagPredicate newPredicate = getTagPredicate(comparisonExpression, right, tagColumnOrdinal);
    return PredicateParseResult.tag(combinePredicates(oldPredicate, newPredicate));
  }

  private static void validateAttributeComparison(final LikePredicate likePredicate) {
    validateAttributePredicateStringValue(likePredicate.getPattern());
    if (likePredicate.getEscape().isPresent()) {
      validateAttributePredicateStringValue(likePredicate.getEscape().get());
    }
  }

  private static void validateAttributeComparison(final InPredicate inPredicate) {
    if (!(inPredicate.getValueList() instanceof InListExpression)) {
      throw new SemanticException(
          DataNodeQueryMessages.THE_RIGHT_HAND_VALUE_OF_ATTRIBUTE_PREDICATE_MUST_BE_A_STRING
              + inPredicate.getValueList());
    }
    for (final Expression expression :
        ((InListExpression) inPredicate.getValueList()).getValues()) {
      validateAttributePredicateStringValue(expression);
    }
  }

  private static void validateAttributeComparison(final ComparisonExpression comparisonExpression) {
    switch (comparisonExpression.getOperator()) {
      case EQUAL:
      case NOT_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        break;
      case IS_DISTINCT_FROM:
      default:
        throw new SemanticException(
            DataNodeQueryMessages.THE_OPERATOR_OF_ATTRIBUTE_PREDICATE_MUST_BE_FOR
                + comparisonExpression.getRight());
    }

    validateAttributePredicateStringValue(comparisonExpression.getRight());
  }

  private static void validateAttributePredicateStringValue(final Expression expression) {
    if (expression instanceof NullLiteral) {
      throw new SemanticException(
          DataNodeQueryMessages
              .THE_RIGHT_HAND_VALUE_OF_ATTRIBUTE_PREDICATE_CANNOT_BE_NULL_WITH_COMPARISON_OPERATOR);
    }
    if (!(expression instanceof StringLiteral)) {
      throw new SemanticException(
          DataNodeQueryMessages.THE_RIGHT_HAND_VALUE_OF_ATTRIBUTE_PREDICATE_MUST_BE_A_STRING
              + expression);
    }
  }

  private static String getTimeColumnName(final TsTable table) {
    final TsTableColumnSchema timeColumnSchema = table.getTimeColumnSchema();
    if (Objects.isNull(timeColumnSchema)) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.THE_TABLE_S_DOES_NOT_CONTAIN_A_TIME_COLUMN,
              table.getTableName()));
    }
    return timeColumnSchema.getColumnName();
  }

  private static TagPredicate getTagPredicate(
      ComparisonExpression comparisonExpression, Expression right, int tagColumnOrdinal) {
    if (comparisonExpression.getOperator() != ComparisonExpression.Operator.EQUAL) {
      throw new SemanticException(
          DataNodeQueryMessages.THE_OPERATOR_OF_TAG_PREDICATE_MUST_BE_FOR + right);
    }

    String rightHandValue;
    if (right instanceof StringLiteral) {
      rightHandValue = ((StringLiteral) right).getValue();
    } else if (right instanceof NullLiteral) {
      throw new SemanticException(
          DataNodeQueryMessages
              .THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_CANNOT_BE_NULL_WITH_COMPARISON_OPERATOR);
    } else {
      throw new SemanticException(
          DataNodeQueryMessages.THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_MUST_BE_A_STRING + right);
    }
    // the first segment is the table name, so + 1
    return new SegmentExactMatch(rightHandValue, tagColumnOrdinal + 1);
  }

  private static Expression toSymbolReferenceExpression(final Expression expression) {
    return ExpressionTreeRewriter.rewriteWith(
        new ExpressionRewriter<>() {
          @Override
          public Expression rewriteIdentifier(
              final Identifier node,
              final Void context,
              final ExpressionTreeRewriter<Void> treeRewriter) {
            return new SymbolReference(node.getValue());
          }
        },
        expression);
  }

  private static class PredicateParseContext {
    private final TsTable table;
    private final Queue<Expression> expressionQueue = new LinkedList<>();
    private final TimeRange timeRange;
    private Expression rootExpression;
    private TagPredicate tagPredicate;
    private List<Expression> deviceFilterExpressions;
    private List<String> attributeColumns;

    private PredicateParseContext(final TsTable table, final TimeRange timeRange) {
      this.table = table;
      this.timeRange = timeRange;
    }
  }

  private static class PredicateParseResult {
    private final TagPredicate tagPredicate;
    private final String attributeColumn;
    private final boolean filterDevice;

    private PredicateParseResult(
        final TagPredicate tagPredicate, final String attributeColumn, final boolean filterDevice) {
      this.tagPredicate = tagPredicate;
      this.attributeColumn = attributeColumn;
      this.filterDevice = filterDevice;
    }

    private static PredicateParseResult time(final TagPredicate tagPredicate) {
      return new PredicateParseResult(tagPredicate, null, false);
    }

    private static PredicateParseResult empty(final TagPredicate tagPredicate) {
      return new PredicateParseResult(tagPredicate, null, false);
    }

    private static PredicateParseResult tag(final TagPredicate tagPredicate) {
      return new PredicateParseResult(tagPredicate, null, true);
    }

    private static PredicateParseResult attribute(
        final String attributeColumn, final TagPredicate tagPredicate) {
      return new PredicateParseResult(tagPredicate, attributeColumn, true);
    }

    private boolean shouldQueryDevice() {
      return Objects.nonNull(attributeColumn);
    }

    private boolean shouldFilterDevice() {
      return filterDevice;
    }
  }

  public interface DataPartitionQueryFunc {

    DataPartition queryDataPartition(
        final List<DataPartitionQueryParam> dataPartitionQueryParams, final String userName);
  }

  public interface DataPartitionQueryParamComputation {

    List<DataPartitionQueryParam> compute(InsertBaseStatement statement, MPPQueryContext context);
  }
}
