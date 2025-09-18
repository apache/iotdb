/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAIDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowLoadedModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryDataSetHandler {

  private QueryDataSetHandler() {}

  /**
   * @param actualRowSizeLimit max number of rows to return. no limit when actualRowSizeLimit <= 0.
   */
  public static Response fillQueryDataSet(
      IQueryExecution queryExecution, Statement statement, int actualRowSizeLimit)
      throws IoTDBException {
    if (statement instanceof ShowStatement
        || statement instanceof ShowModelsStatement
        || statement instanceof ShowAIDevicesStatement
        || statement instanceof ShowLoadedModelsStatement
        || statement instanceof AuthorStatement
        || statement instanceof GetRegionIdStatement) {
      return fillShowPlanDataSet(queryExecution, actualRowSizeLimit);
    } else if (statement instanceof QueryStatement) {
      if (((QueryStatement) statement).isAggregationQuery()
          && !((QueryStatement) statement).isGroupByTime()) {
        return fillAggregationPlanDataSet(queryExecution, actualRowSizeLimit);
      }
      return fillDataSetWithTimestamps(queryExecution, actualRowSizeLimit, 1);
    }
    return Response.ok()
        .entity(
            new ExecutionStatus()
                .code(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode())
                .message(
                    String.format(
                        "unsupported query data type: %s", statement.getType().toString())))
        .build();
  }

  public static Response fillDataSetWithTimestamps(
      IQueryExecution queryExecution, final int actualRowSizeLimit, final long timePrecision)
      throws IoTDBException {
    org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet();

    return fillQueryDataSetWithTimestamps(
        queryExecution, actualRowSizeLimit, targetDataSet, timePrecision);
  }

  public static Response fillAggregationPlanDataSet(
      IQueryExecution queryExecution, final int actualRowSizeLimit) throws IoTDBException {

    org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet();

    DatasetHeader datasetHeader = queryExecution.getDatasetHeader();
    int[] targetDataSetIndexToSourceDataSetIndex = new int[datasetHeader.getRespColumns().size()];
    for (int i = 0; i < datasetHeader.getRespColumns().size(); i++) {
      targetDataSet.addExpressionsItem(datasetHeader.getRespColumns().get(i));
      targetDataSet.addValuesItem(new ArrayList<>());
      targetDataSet.addDataTypesItem(
          datasetHeader.getColumnHeaders().get(i).getColumnType().name());
      targetDataSetIndexToSourceDataSetIndex[i] =
          datasetHeader.getColumnNameIndexMap().get(datasetHeader.getRespColumns().get(i));
    }

    return fillQueryDataSetWithoutTimestamps(
        queryExecution, targetDataSetIndexToSourceDataSetIndex, actualRowSizeLimit, targetDataSet);
  }

  private static Response fillShowPlanDataSet(
      IQueryExecution queryExecution, final int actualRowSizeLimit) throws IoTDBException {
    org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet();
    int[] targetDataSetIndexToSourceDataSetIndex =
        new int[queryExecution.getDatasetHeader().getRespColumns().size()];
    initTargetDatasetOrderByOrderWithSourceDataSet(
        queryExecution.getDatasetHeader(), targetDataSetIndexToSourceDataSetIndex, targetDataSet);

    return fillQueryDataSetWithoutTimestamps(
        queryExecution, targetDataSetIndexToSourceDataSetIndex, actualRowSizeLimit, targetDataSet);
  }

  private static void initTargetDatasetOrderByOrderWithSourceDataSet(
      DatasetHeader datasetHeader,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet) {
    if (datasetHeader.getRespColumns() != null) {
      for (int i = 0; i < datasetHeader.getRespColumns().size(); i++) {
        targetDataSet.addColumnNamesItem(datasetHeader.getRespColumns().get(i));
        targetDataSet.addValuesItem(new ArrayList<>());
        targetDataSetIndexToSourceDataSetIndex[i] = i;
      }
    }
  }

  private static Response fillQueryDataSetWithTimestamps(
      IQueryExecution queryExecution,
      int actualRowSizeLimit,
      org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet,
      final long timePrecision)
      throws IoTDBException {
    int fetched = 0;
    int columnNum = queryExecution.getOutputValueColumnCount();

    DatasetHeader header = queryExecution.getDatasetHeader();
    List<String> resultColumns = header.getRespColumns();
    List<TSDataType> dataTypes = header.getRespDataTypes();
    Map<String, Integer> headerMap = header.getColumnNameIndexMap();
    for (int i = 0; i < resultColumns.size(); i++) {
      targetDataSet.addExpressionsItem(resultColumns.get(i));
      targetDataSet.addDataTypesItem(dataTypes.get(i).name());
      targetDataSet.addValuesItem(new ArrayList<>());
    }

    while (true) {
      if (0 < actualRowSizeLimit && actualRowSizeLimit <= fetched) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode())
                    .message(
                        String.format(
                            "Dataset row size exceeded the given max row size (%d)",
                            actualRowSizeLimit)))
            .build();
      }
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent() || optionalTsBlock.get().isEmpty()) {
        if (fetched == 0) {
          targetDataSet.setTimestamps(new ArrayList<>());
          targetDataSet.setValues(new ArrayList<>());
          return Response.ok().entity(targetDataSet).build();
        }
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();
      int currentCount = tsBlock.getPositionCount();
      // time column
      for (int i = 0; i < currentCount; i++) {
        targetDataSet.addTimestampsItem(
            timePrecision == 1
                ? tsBlock.getTimeByIndex(i)
                : tsBlock.getTimeByIndex(i) / timePrecision);
      }
      for (int k = 0; k < resultColumns.size(); k++) {
        Column column = tsBlock.getColumn(headerMap.get(resultColumns.get(k)));
        List<Object> targetDataSetColumn = targetDataSet.getValues().get(k);
        for (int i = 0; i < currentCount; i++) {
          fetched++;
          if (column.isNull(i)) {
            targetDataSetColumn.add(null);
          } else {
            addTypedValueToTarget(targetDataSet.getDataTypes(), k, i, targetDataSetColumn, column);
          }
        }
        if (k != columnNum - 1) {
          fetched -= currentCount;
        }
      }
    }
    return Response.ok().entity(targetDataSet).build();
  }

  private static Response fillQueryDataSetWithoutTimestamps(
      IQueryExecution queryExecution,
      int[] targetDataSetIndexToSourceDataSetIndex,
      int actualRowSizeLimit,
      org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet targetDataSet)
      throws IoTDBException {
    int fetched = 0;
    int columnNum = queryExecution.getOutputValueColumnCount();
    List<String> dataTypes = targetDataSet.getDataTypes();
    while (true) {
      if (0 < actualRowSizeLimit && actualRowSizeLimit <= fetched) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode())
                    .message(
                        String.format(
                            "Dataset row size exceeded the given max row size (%d)",
                            actualRowSizeLimit)))
            .build();
      }
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        if (fetched == 0) {
          targetDataSet.setValues(new ArrayList<>());
          return Response.ok().entity(targetDataSet).build();
        }
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();
      int currentCount = tsBlock.getPositionCount();
      if (currentCount == 0) {
        targetDataSet.setValues(new ArrayList<>());
        return Response.ok().entity(targetDataSet).build();
      }
      for (int k = 0; k < columnNum; k++) {
        Column column = tsBlock.getColumn(targetDataSetIndexToSourceDataSetIndex[k]);
        List<Object> targetDataSetColumn = targetDataSet.getValues().get(k);
        for (int i = 0; i < currentCount; i++) {
          fetched++;
          if (column.isNull(i)) {
            targetDataSetColumn.add(null);
          } else {
            addTypedValueToTarget(dataTypes, k, i, targetDataSetColumn, column);
          }
        }
        if (k != columnNum - 1) {
          fetched -= currentCount;
        }
      }
    }
    return Response.ok().entity(targetDataSet).build();
  }

  private static void addTypedValueToTarget(
      List<String> dataTypes,
      int colIndex,
      int rowIndex,
      List<Object> targetColumnList,
      Column column) {
    String dataTypeName = dataTypes != null ? dataTypes.get(colIndex) : null;

    if (TSDataType.TEXT.name().equals(dataTypeName)) {
      targetColumnList.add(column.getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET));
    } else if (TSDataType.DATE.name().equals(dataTypeName)) {
      int intValue = column.getInt(rowIndex);
      targetColumnList.add(DateUtils.formatDate(intValue));
    } else if (TSDataType.BLOB.name().equals(dataTypeName)) {
      byte[] v = column.getBinary(rowIndex).getValues();
      targetColumnList.add(BytesUtils.parseBlobByteArrayToString(v));
    } else {
      targetColumnList.add(
          column.getDataType().equals(TSDataType.TEXT)
              ? column.getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET)
              : column.getObject(rowIndex));
    }
  }

  public static Response fillGrafanaVariablesResult(
      IQueryExecution queryExecution, Statement statement) throws IoTDBException {
    List<String> results = new ArrayList<>();
    Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
    if (!optionalTsBlock.isPresent()) {
      return Response.ok().entity(results).build();
    }
    TsBlock tsBlock = optionalTsBlock.get();
    int currentCount = tsBlock.getPositionCount();
    Column column = tsBlock.getColumn(0);

    for (int i = 0; i < currentCount; i++) {
      String nodePaths = column.getObject(i).toString();
      if (statement instanceof ShowChildPathsStatement) {
        String[] nodeSubPath = nodePaths.split("\\.");
        results.add(nodeSubPath[nodeSubPath.length - 1]);
      } else {
        results.add(nodePaths);
      }
    }
    return Response.ok().entity(results).build();
  }

  public static Response fillGrafanaNodesResult(IQueryExecution queryExecution)
      throws IoTDBException {
    List<String> nodes = new ArrayList<>();
    Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
    if (!optionalTsBlock.isPresent()) {
      return Response.ok().entity(nodes).build();
    }
    TsBlock tsBlock = optionalTsBlock.get();
    int currentCount = tsBlock.getPositionCount();
    Column column = tsBlock.getColumn(0);

    for (int i = 0; i < currentCount; i++) {
      String nodePaths = column.getObject(i).toString();
      String[] nodeSubPath = nodePaths.split("\\.");
      nodes.add(nodeSubPath[nodeSubPath.length - 1]);
    }
    return Response.ok().entity(nodes).build();
  }
}
