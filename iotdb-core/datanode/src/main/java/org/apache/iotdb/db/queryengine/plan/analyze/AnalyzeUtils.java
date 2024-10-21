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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
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
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
    } else if (statement instanceof InsertRowStatement) {
      InsertRowStatement insertRowStatement = (InsertRowStatement) statement;
      return computeDataPartitionParams(
          Collections.singletonMap(
              insertRowStatement.getTableDeviceID(),
              Collections.singleton(insertRowStatement.getTimePartitionSlot())),
          getDatabaseName(statement, context));
    } else if (statement instanceof InsertRowsStatement) {
      InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      Map<IDeviceID, Set<TTimePartitionSlot>> timePartitionSlotMap = new HashMap<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        timePartitionSlotMap
            .computeIfAbsent(insertRowStatement.getTableDeviceID(), id -> new HashSet<>())
            .add(insertRowStatement.getTimePartitionSlot());
      }
      return computeDataPartitionParams(timePartitionSlotMap, getDatabaseName(statement, context));
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

  public interface DataPartitionQueryFunc {

    DataPartition queryDataPartition(
        List<DataPartitionQueryParam> dataPartitionQueryParams, String userName);
  }

  public interface DataPartitionQueryParamComputation {

    List<DataPartitionQueryParam> compute(InsertBaseStatement statement, MPPQueryContext context);
  }
}
