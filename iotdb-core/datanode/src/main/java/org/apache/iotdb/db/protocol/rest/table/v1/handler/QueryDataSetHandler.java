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

package org.apache.iotdb.db.protocol.rest.table.v1.handler;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.table.v1.model.QueryDataSet;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
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
    if (statement instanceof Query) {
      return fillQueryDataSet(queryExecution, actualRowSizeLimit);
    } else {
      return fillOtherDataSet(queryExecution, actualRowSizeLimit);
    }
  }

  public static Response fillQueryDataSet(
      IQueryExecution queryExecution, final int actualRowSizeLimit) throws IoTDBException {
    QueryDataSet targetDataSet = new QueryDataSet();
    int fetched = 0;
    int columnNum = queryExecution.getOutputValueColumnCount();

    DatasetHeader header = queryExecution.getDatasetHeader();
    List<String> resultColumns = header.getRespColumns();
    List<TSDataType> dataTypes = header.getRespDataTypes();
    Map<String, Integer> headerMap = header.getColumnNameIndexMap();
    for (int i = 0; i < resultColumns.size(); i++) {
      targetDataSet.addColumnNamesItem(resultColumns.get(i));
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
          targetDataSet.setValues(new ArrayList<>());
          return Response.ok().entity(targetDataSet).build();
        }
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();
      int currentCount = tsBlock.getPositionCount();

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
    targetDataSet.setValues(convertColumnToRow(targetDataSet.getValues()));
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

  private static Response fillOtherDataSet(
      IQueryExecution queryExecution, final int actualRowSizeLimit) throws IoTDBException {
    QueryDataSet targetDataSet = new QueryDataSet();
    int[] targetDataSetIndexToSourceDataSetIndex =
        new int[queryExecution.getDatasetHeader().getRespColumns().size()];
    initTargetDatasetOrderByOrderWithSourceDataSet(
        queryExecution.getDatasetHeader(), targetDataSetIndexToSourceDataSetIndex, targetDataSet);

    return fillOtherQueryDataSet(
        queryExecution, targetDataSetIndexToSourceDataSetIndex, actualRowSizeLimit, targetDataSet);
  }

  private static void initTargetDatasetOrderByOrderWithSourceDataSet(
      DatasetHeader datasetHeader,
      int[] targetDataSetIndexToSourceDataSetIndex,
      QueryDataSet targetDataSet) {
    if (datasetHeader.getRespColumns() != null) {
      for (int i = 0; i < datasetHeader.getRespColumns().size(); i++) {
        targetDataSet.addColumnNamesItem(datasetHeader.getRespColumns().get(i));
        targetDataSet.addValuesItem(new ArrayList<>());
        targetDataSet.addDataTypesItem(datasetHeader.getRespDataTypes().get(i).name());
        targetDataSetIndexToSourceDataSetIndex[i] = i;
      }
    }
  }

  private static Response fillOtherQueryDataSet(
      IQueryExecution queryExecution,
      int[] targetDataSetIndexToSourceDataSetIndex,
      int actualRowSizeLimit,
      QueryDataSet targetDataSet)
      throws IoTDBException {
    int fetched = 0;
    int columnNum = queryExecution.getOutputValueColumnCount();
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
            addTypedValueToTarget(targetDataSet.getDataTypes(), k, i, targetDataSetColumn, column);
          }
        }
        if (k != columnNum - 1) {
          fetched -= currentCount;
        }
      }
    }
    targetDataSet.setValues(convertColumnToRow(targetDataSet.getValues()));
    return Response.ok().entity(targetDataSet).build();
  }

  private static List<List<Object>> convertColumnToRow(List<List<Object>> values) {
    List<List<Object>> result = new ArrayList<>();

    if (values.isEmpty() || values.get(0).isEmpty()) {
      return result;
    }

    int numRows = values.get(0).size();
    for (int i = 0; i < numRows; i++) {
      List<Object> newRow = new ArrayList<>();
      for (List<Object> value : values) {
        newRow.add(value.get(i));
      }
      result.add(newRow);
    }
    return result;
  }
}
