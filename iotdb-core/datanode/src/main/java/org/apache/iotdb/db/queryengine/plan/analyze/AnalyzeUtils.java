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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TimeRange;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.And;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.SegmentExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
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
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;

public class AnalyzeUtils {

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

  public static String getDatabaseName(InsertBaseStatement statement, MPPQueryContext context) {
    if (statement.getDatabaseName().isPresent()) {
      return statement.getDatabaseName().get();
    }
    if (context != null && context.getDatabaseName().isPresent()) {
      return context.getDatabaseName().get();
    }
    return null;
  }

  public static String getDatabaseNameForTableWithRoot(
      InsertBaseStatement statement, MPPQueryContext context) {
    if (statement.getDatabaseName().isPresent()) {
      return PATH_ROOT + PATH_SEPARATOR + statement.getDatabaseName().get();
    }
    if (context != null && context.getDatabaseName().isPresent()) {
      return PATH_ROOT + PATH_SEPARATOR + context.getDatabaseName().get();
    }
    return null;
  }

  public static List<DataPartitionQueryParam> computeTableDataPartitionParams(
      InsertBaseStatement statement, MPPQueryContext context) {
    if (statement instanceof InsertTabletStatement) {
      InsertTabletStatement insertTabletStatement = (InsertTabletStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
        timePartitionSlotMap
            .computeIfAbsent(insertTabletStatement.getTableDeviceID(i), id -> new HashSet<>())
            .add(insertTabletStatement.getTimePartitionSlot(i));
      }
      return computeDataPartitionParams(
          timePartitionSlotMap, getDatabaseNameForTableWithRoot(statement, context));
    } else if (statement instanceof InsertMultiTabletsStatement) {
      InsertMultiTabletsStatement insertMultiTabletsStatement =
          (InsertMultiTabletsStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
          timePartitionSlotMap
              .computeIfAbsent(insertTabletStatement.getTableDeviceID(i), id -> new HashSet<>())
              .add(insertTabletStatement.getTimePartitionSlot(i));
        }
      }
      return computeDataPartitionParams(
          timePartitionSlotMap, getDatabaseNameForTableWithRoot(statement, context));
    } else if (statement instanceof InsertRowStatement) {
      InsertRowStatement insertRowStatement = (InsertRowStatement) statement;
      return computeDataPartitionParams(
          Collections.singletonMap(
              insertRowStatement.getTableDeviceID(),
              Collections.singleton(insertRowStatement.getTimePartitionSlot())),
          getDatabaseNameForTableWithRoot(statement, context));
    } else if (statement instanceof InsertRowsStatement) {
      InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        timePartitionSlotMap
            .computeIfAbsent(insertRowStatement.getTableDeviceID(), id -> new HashSet<>())
            .add(insertRowStatement.getTimePartitionSlot());
      }
      return computeDataPartitionParams(
          timePartitionSlotMap, getDatabaseNameForTableWithRoot(statement, context));
    }
    throw new UnsupportedOperationException("computeDataPartitionParams for " + statement);
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
    throw new UnsupportedOperationException("computeDataPartitionParams for " + statement);
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
      IAnalysis analysis,
      List<DataPartitionQueryParam> dataPartitionQueryParams,
      String userName,
      DataPartitionQueryFunc partitionQueryFunc) {

    DataPartition dataPartition =
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

  public static void analyzeDelete(Delete node, MPPQueryContext queryContext) {
    queryContext.setQueryType(QueryType.WRITE);
    validateSchema(node, queryContext);

    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID); ) {
      // TODO: may use time and db/table to filter
      TRegionRouteMapResp latestRegionRouteMap = configNodeClient.getLatestRegionRouteMap();
      Set<TRegionReplicaSet> replicaSets = new HashSet<>();
      latestRegionRouteMap.getRegionRouteMap().entrySet().stream()
          .filter(e -> e.getKey().getType() == TConsensusGroupType.DataRegion)
          .forEach(e -> replicaSets.add(e.getValue()));
      node.setReplicaSets(replicaSets);
    } catch (Exception e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.CAN_NOT_CONNECT_CONFIGNODE.getStatusCode());
    }
  }

  @SuppressWarnings("java:S3655") // optional is checked
  private static void validateSchema(Delete node, MPPQueryContext queryContext) {
    String tableName = node.getTable().getName().getSuffix();
    String databaseName;
    if (node.getTable().getName().getPrefix().isPresent()) {
      databaseName = node.getTable().getName().getPrefix().get().toString();
    } else if (queryContext.getDatabaseName().isPresent()) {
      databaseName = queryContext.getDatabaseName().get();
    } else {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    node.setDatabaseName(databaseName);

    TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName);
    if (table == null) {
      throw new SemanticException("Table " + tableName + " not found");
    }

    node.setTableDeletionEntries(parseExpressions2ModEntries(node.getWhere().orElse(null), table));
  }

  public static List<TableDeletionEntry> parseExpressions2ModEntries(
      final Expression expression, final TsTable table) {
    return toDisjunctiveNormalForms(expression).stream()
        .map(disjunctiveNormalForm -> parsePredicate(disjunctiveNormalForm, table))
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
      throw new SemanticException("Unsupported operator: " + logicalExpression.getOperator());
    }
  }

  private static List<Expression> crossProductOfDisjunctiveNormalForms(
      List<Expression> leftList, List<Expression> rightList, Operator operator) {
    List<Expression> results = new ArrayList<>();
    for (Expression leftExp : leftList) {
      List<Expression> terms = new ArrayList<>();
      if (leftExp instanceof LogicalExpression) {
        terms.addAll(((LogicalExpression) leftExp).getTerms());
      } else {
        terms.add(leftExp);
      }

      for (Expression rightExp : rightList) {
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

  private static TableDeletionEntry parsePredicate(Expression expression, TsTable table) {
    if (expression == null) {
      return new TableDeletionEntry(
          new DeletionPredicate(table.getTableName()),
          new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE, true).toTsFileTimeRange());
    }

    Queue<Expression> expressionQueue = new LinkedList<>();
    expressionQueue.add(expression);
    DeletionPredicate predicate = new DeletionPredicate(table.getTableName());
    IDPredicate idPredicate = null;
    TimeRange timeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE, true);
    while (!expressionQueue.isEmpty()) {
      Expression currExp = expressionQueue.remove();
      if (currExp instanceof LogicalExpression) {
        parseAndPredicate(((LogicalExpression) currExp), expressionQueue);
      } else if (currExp instanceof ComparisonExpression) {
        idPredicate =
            parseComparison(((ComparisonExpression) currExp), timeRange, idPredicate, table);
      } else if (currExp instanceof IsNullPredicate) {
        idPredicate = parseIsNull((IsNullPredicate) currExp, idPredicate, table);
      } else {
        throw new SemanticException("Unsupported expression: " + currExp + " in " + expression);
      }
    }
    if (idPredicate != null) {
      predicate.setIdPredicate(idPredicate);
    }
    if (timeRange.getStartTime() > timeRange.getEndTime()) {
      throw new SemanticException(
          String.format(
              "Start time %d is greater than end time %d",
              timeRange.getStartTime(), timeRange.getEndTime()));
    }

    return new TableDeletionEntry(predicate, timeRange.toTsFileTimeRange());
  }

  private static void parseAndPredicate(
      LogicalExpression expression, Queue<Expression> expressionQueue) {
    if (expression.getOperator() != Operator.AND) {
      throw new SemanticException("Only support AND operator in deletion");
    }
    expressionQueue.addAll(expression.getTerms());
  }

  private static IDPredicate parseIsNull(
      IsNullPredicate isNullPredicate, IDPredicate oldPredicate, TsTable table) {
    Expression leftHandExp = isNullPredicate.getValue();
    if (!(leftHandExp instanceof Identifier)) {
      throw new SemanticException("Left hand expression is not an identifier: " + leftHandExp);
    }
    String columnName = ((Identifier) leftHandExp).getValue();
    int idColumnOrdinal = table.getIdColumnOrdinal(columnName);
    if (idColumnOrdinal == -1) {
      throw new SemanticException(
          "The column '" + columnName + "' does not exist or is not an id column");
    }

    // the first segment is the table name, so + 1
    IDPredicate newPredicate = new SegmentExactMatch(null, idColumnOrdinal + 1);
    return combinePredicates(oldPredicate, newPredicate);
  }

  private static IDPredicate combinePredicates(IDPredicate oldPredicate, IDPredicate newPredicate) {
    if (oldPredicate == null) {
      return newPredicate;
    }
    if (oldPredicate instanceof IDPredicate.And) {
      ((And) oldPredicate).add(newPredicate);
      return oldPredicate;
    }
    return new IDPredicate.And(oldPredicate, newPredicate);
  }

  private static IDPredicate parseComparison(
      ComparisonExpression comparisonExpression,
      TimeRange timeRange,
      IDPredicate oldPredicate,
      TsTable table) {
    Expression left = comparisonExpression.getLeft();
    Expression right = comparisonExpression.getRight();
    if (!(left instanceof Identifier)) {
      throw new SemanticException("The left hand value must be an identifier: " + left);
    }
    Identifier identifier = (Identifier) left;
    // time predicate
    if (identifier.getValue().equalsIgnoreCase("time")) {
      long rightHandValue;
      if (right instanceof LongLiteral) {
        rightHandValue = ((LongLiteral) right).getParsedValue();
      } else {
        throw new SemanticException(
            "The right hand value of time predicate must be a long: " + right);
      }

      switch (comparisonExpression.getOperator()) {
        case LESS_THAN:
          timeRange.setEndTime(Math.min(timeRange.getEndTime(), rightHandValue - 1));
          break;
        case LESS_THAN_OR_EQUAL:
          timeRange.setEndTime(Math.min(timeRange.getEndTime(), rightHandValue));
          break;
        case GREATER_THAN:
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
              "The operator of time predicate must be <, <=, >, or >=: " + right);
      }

      return oldPredicate;
    }
    // id predicate
    String columnName = identifier.getValue();
    int idColumnOrdinal = table.getIdColumnOrdinal(columnName);
    if (idColumnOrdinal == -1) {
      throw new SemanticException(
          "The column '" + columnName + "' does not exist or is not an id column");
    }

    IDPredicate newPredicate = getIdPredicate(comparisonExpression, right, idColumnOrdinal);
    return combinePredicates(oldPredicate, newPredicate);
  }

  private static IDPredicate getIdPredicate(
      ComparisonExpression comparisonExpression, Expression right, int idColumnOrdinal) {
    if (comparisonExpression.getOperator() != ComparisonExpression.Operator.EQUAL) {
      throw new SemanticException("The operator of id predicate must be '=' for " + right);
    }

    String rightHandValue;
    if (right instanceof StringLiteral) {
      rightHandValue = ((StringLiteral) right).getValue();
    } else if (right instanceof NullLiteral) {
      throw new SemanticException(
          "The right hand value of id predicate cannot be null with '=' operator, please use 'IS NULL' instead");
    } else {
      throw new SemanticException(
          "The right hand value of id predicate must be a string: " + right);
    }
    // the first segment is the table name, so + 1
    return new SegmentExactMatch(rightHandValue, idColumnOrdinal + 1);
  }

  public interface DataPartitionQueryFunc {

    DataPartition queryDataPartition(
        List<DataPartitionQueryParam> dataPartitionQueryParams, String userName);
  }

  public interface DataPartitionQueryParamComputation {

    List<DataPartitionQueryParam> compute(InsertBaseStatement statement, MPPQueryContext context);
  }
}
