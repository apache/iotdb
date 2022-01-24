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

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.dataset.ShowDevicesDataSet;
import org.apache.iotdb.db.query.dataset.ShowTimeseriesDataSet;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByLevelDataSet;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class QueryDataSetHandler {

  private QueryDataSetHandler() {}

  /**
   * @param actualRowSizeLimit max number of rows to return. no limit when actualRowSizeLimit <= 0.
   */
  public static Response fillQueryDataSet(
      QueryDataSet sourceDataSet, PhysicalPlan physicalPlan, int actualRowSizeLimit)
      throws IOException {
    if (sourceDataSet instanceof ShowDevicesDataSet
        || (sourceDataSet instanceof ListDataSet && !(physicalPlan instanceof LastQueryPlan))
        || sourceDataSet instanceof ShowTimeseriesDataSet
        || (sourceDataSet instanceof SingleDataSet && !(physicalPlan instanceof AggregationPlan))) {
      return fillShowPlanDataSet(sourceDataSet, actualRowSizeLimit);
    } else if (sourceDataSet instanceof ListDataSet) {
      return fillLastQueryPlanDataSet(sourceDataSet, actualRowSizeLimit);
    } else if (sourceDataSet instanceof SingleDataSet
        && ((AggregationPlan) physicalPlan).getLevels() != null) {
      return fillAggregationPlanDataSet(
          sourceDataSet, (AggregationPlan) physicalPlan, actualRowSizeLimit);
    } else if (sourceDataSet instanceof GroupByLevelDataSet) {
      return fillGroupByLevelDataSet(sourceDataSet, actualRowSizeLimit);
    } else if (physicalPlan instanceof QueryPlan) {
      return fillDataSetWithTimestamps(sourceDataSet, (QueryPlan) physicalPlan, actualRowSizeLimit);
    } else {
      return Response.ok()
          .entity(
              new ExecutionStatus()
                  .code(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode())
                  .message(
                      String.format(
                          "unsupported query data type: %s", sourceDataSet.getClass().getName())))
          .build();
    }
  }

  public static Response fillDataSetWithTimestamps(
      QueryDataSet sourceDataSet, QueryPlan queryPlan, final int actualRowSizeLimit)
      throws IOException {
    org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();

    List<ResultColumn> resultColumns = queryPlan.getResultColumns();
    int[] targetDataSetIndexToSourceDataSetIndex = new int[resultColumns.size()];
    Map<String, Integer> sourcePathToQueryDataSetIndex = queryPlan.getPathToIndex();
    for (int i = 0; i < resultColumns.size(); i++) {
      ResultColumn resultColumn = resultColumns.get(i);
      targetDataSet.addExpressionsItem(resultColumn.getResultColumnName());
      targetDataSet.addValuesItem(new ArrayList<>());
      targetDataSetIndexToSourceDataSetIndex[i] =
          sourcePathToQueryDataSetIndex.get(resultColumn.getResultColumnName());
    }

    return fillQueryDataSetWithTimestamps(
        sourceDataSet, actualRowSizeLimit, targetDataSetIndexToSourceDataSetIndex, targetDataSet);
  }

  public static Response fillLastQueryPlanDataSet(
      QueryDataSet sourceDataSet, final int actualRowSizeLimit) throws IOException {
    int[] targetDataSetIndexToSourceDataSetIndex = new int[sourceDataSet.getPaths().size()];
    org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();
    initTargetDatasetOrderByOrderWithSourceDataSet(
        sourceDataSet, targetDataSetIndexToSourceDataSetIndex, targetDataSet);

    return fillQueryDataSetWithTimestamps(
        sourceDataSet, actualRowSizeLimit, targetDataSetIndexToSourceDataSetIndex, targetDataSet);
  }

  public static Response fillGroupByLevelDataSet(
      QueryDataSet sourceDataSet, final int actualRowSizeLimit) throws IOException {
    int[] targetDataSetIndexToSourceDataSetIndex = new int[sourceDataSet.getPaths().size()];
    org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();
    initTargetDatasetExpByOrderWithSourceDataSet(
        sourceDataSet, targetDataSetIndexToSourceDataSetIndex, targetDataSet);

    return fillQueryDataSetWithTimestamps(
        sourceDataSet, actualRowSizeLimit, targetDataSetIndexToSourceDataSetIndex, targetDataSet);
  }

  private static Response fillAggregationPlanDataSet(
      QueryDataSet dataSet, AggregationPlan aggregationPlan, final int actualRowSizeLimit)
      throws IOException {
    Map<String, AggregateResult> groupPathsResultMap = aggregationPlan.getGroupPathsResultMap();
    int[] targetDataSetIndexToSourceDataSetIndex = new int[groupPathsResultMap.size()];

    org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();

    Map<String, Integer> sourcePathToSourceQueryDataSetIndex = aggregationPlan.getPathToIndex();
    Iterator<Entry<String, AggregateResult>> iterator = groupPathsResultMap.entrySet().iterator();
    for (int i = 0; iterator.hasNext(); i++) {
      Entry<String, AggregateResult> next = iterator.next();
      targetDataSet.addColumnNamesItem(next.getKey());
      targetDataSet.addValuesItem(new ArrayList<>());
      targetDataSetIndexToSourceDataSetIndex[i] =
          sourcePathToSourceQueryDataSetIndex.get(next.getKey());
    }

    return fillQueryDataSetWithoutTimestamps(
        dataSet, actualRowSizeLimit, targetDataSetIndexToSourceDataSetIndex, targetDataSet);
  }

  private static Response fillShowPlanDataSet(
      QueryDataSet sourceDataSet, final int actualRowSizeLimit) throws IOException {
    int[] targetDataSetIndexToSourceDataSetIndex = new int[sourceDataSet.getPaths().size()];
    org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet =
        new org.apache.iotdb.db.protocol.rest.model.QueryDataSet();
    initTargetDatasetOrderByOrderWithSourceDataSet(
        sourceDataSet, targetDataSetIndexToSourceDataSetIndex, targetDataSet);

    return fillQueryDataSetWithoutTimestamps(
        sourceDataSet, actualRowSizeLimit, targetDataSetIndexToSourceDataSetIndex, targetDataSet);
  }

  private static void initTargetDatasetOrderByOrderWithSourceDataSet(
      QueryDataSet sourceDataSet,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet) {
    if (sourceDataSet.getPaths() != null) {
      for (int i = 0; i < sourceDataSet.getPaths().size(); i++) {
        Path path = sourceDataSet.getPaths().get(i);
        targetDataSet.addColumnNamesItem(path.getFullPath());
        targetDataSet.addValuesItem(new ArrayList<>());
        targetDataSetIndexToSourceDataSetIndex[i] = i;
      }
    }
  }

  private static void initTargetDatasetExpByOrderWithSourceDataSet(
      QueryDataSet sourceDataSet,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet) {
    if (sourceDataSet.getPaths() != null) {
      for (int i = 0; i < sourceDataSet.getPaths().size(); i++) {
        Path path = sourceDataSet.getPaths().get(i);
        targetDataSet.addExpressionsItem(path.getFullPath());
        targetDataSet.addValuesItem(new ArrayList<>());
        targetDataSetIndexToSourceDataSetIndex[i] = i;
      }
    }
  }

  private static Response fillQueryDataSetWithTimestamps(
      QueryDataSet sourceDataSet,
      int actualRowSizeLimit,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet)
      throws IOException {
    int fetched = 0;

    while (sourceDataSet.hasNext()) {
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

      RowRecord sourceDataSetRowRecord = sourceDataSet.next();
      targetDataSet.addTimestampsItem(sourceDataSetRowRecord.getTimestamp());
      fillSourceRowRecordIntoTargetDataSet(
          sourceDataSetRowRecord, targetDataSetIndexToSourceDataSetIndex, targetDataSet);

      ++fetched;
    }

    return Response.ok().entity(targetDataSet).build();
  }

  private static Response fillQueryDataSetWithoutTimestamps(
      QueryDataSet sourceDataSet,
      int actualRowSizeLimit,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet)
      throws IOException {
    int fetched = 0;

    while (sourceDataSet.hasNext()) {
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

      fillSourceRowRecordIntoTargetDataSet(
          sourceDataSet.next(), targetDataSetIndexToSourceDataSetIndex, targetDataSet);

      fetched++;
    }

    return Response.ok().entity(targetDataSet).build();
  }

  private static void fillSourceRowRecordIntoTargetDataSet(
      RowRecord sourceDataSetRowRecord,
      int[] targetDataSetIndexToSourceDataSetIndex,
      org.apache.iotdb.db.protocol.rest.model.QueryDataSet targetDataSet) {
    final int columnSize =
        targetDataSet.getColumnNames() != null
            ? targetDataSet.getColumnNames().size()
            : targetDataSet.getExpressions().size();

    for (int i = 0; i < columnSize; i++) {
      List<Object> targetDataSetColumn = targetDataSet.getValues().get(i);
      Field sourceDataSetField =
          sourceDataSetRowRecord.getFields().get(targetDataSetIndexToSourceDataSetIndex[i]);

      if (sourceDataSetField == null) {
        targetDataSetColumn.add(null);
      } else {
        targetDataSetColumn.add(
            sourceDataSetField.getDataType().equals(TSDataType.TEXT)
                ? sourceDataSetField.getStringValue()
                : sourceDataSetField.getObjectValue(sourceDataSetField.getDataType()));
      }
    }
  }

  public static Response fillGrafanaVariablesResult(QueryDataSet dataSet, PhysicalPlan physicalPlan)
      throws IOException {
    List<String> results = new ArrayList<>();
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      List<Field> fields = rowRecord.getFields();
      String nodePaths = fields.get(0).getObjectValue(fields.get(0).getDataType()).toString();
      if (physicalPlan instanceof ShowChildPathsPlan) {
        String[] nodeSubPath = nodePaths.split("\\.");
        results.add(nodeSubPath[nodeSubPath.length - 1]);
      } else {
        results.add(nodePaths);
      }
    }
    return Response.ok().entity(results).build();
  }
}
