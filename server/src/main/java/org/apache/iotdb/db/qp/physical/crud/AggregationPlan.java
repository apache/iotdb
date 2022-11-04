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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.utils.GroupByLevelController;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;

import java.util.*;

public class AggregationPlan extends RawDataQueryPlan {

  // e.g., for select count(s1), count(s1), count(s2), count(s2), sum (s1)
  // aggregations are count, count, count, count, sum
  // deduplicatedAggregations are count, count, sum

  private List<String> aggregations = new ArrayList<>();
  private List<String> deduplicatedAggregations = new ArrayList<>();

  private int[] levels;
  private GroupByLevelController groupByLevelController;
  // group by level aggregation result path
  private final Map<String, AggregateResult> groupPathsResultMap = new LinkedHashMap<>();
  private final Map<String, TSDataType> groupedPathToTSDataType = new LinkedHashMap<>();
  private final Map<AggregateResult, AggregateResult> resultToGroupedAhead = new HashMap<>();

  public AggregationPlan() {
    super();
    setOperatorType(Operator.OperatorType.AGGREGATION);
  }

  @Override
  public TSExecuteStatementResp getTSExecuteStatementResp(boolean isJdbcQuery)
      throws TException, MetadataException {
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
    if (isGroupByLevel()) {
      List<String> respColumns = new ArrayList<>();
      List<String> columnsTypes = new ArrayList<>();

      for (Map.Entry<String, AggregateResult> groupPathResult :
          getGroupPathsResultMap().entrySet()) {
        respColumns.add(groupPathResult.getKey());
        columnsTypes.add(groupPathResult.getValue().getResultDataType().toString());
      }
      resp.setColumns(respColumns);
      resp.setDataTypeList(columnsTypes);
    } else {
      resp = super.getTSExecuteStatementResp(isJdbcQuery);
    }
    resp.setIgnoreTimeStamp(true);
    return resp;
  }

  @Override
  public List<TSDataType> getWideQueryHeaders(
      List<String> respColumns, List<String> respSgColumns, boolean isJdbcQuery, BitSet aliasList)
      throws MetadataException {
    List<TSDataType> seriesTypes = new ArrayList<>();
    List<String> aggregations = getAggregations();
    if (aggregations.size() != paths.size()) {
      for (int i = 1; i < paths.size(); i++) {
        aggregations.add(aggregations.get(0));
      }
    }
    for (ResultColumn resultColumn : resultColumns) {
      respColumns.add(resultColumn.getResultColumnName());
    }
    seriesTypes.addAll(SchemaUtils.getSeriesTypesByPaths(paths, aggregations));
    return seriesTypes;
  }

  @Override
  public List<String> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public List<String> getDeduplicatedAggregations() {
    return deduplicatedAggregations;
  }

  public void addDeduplicatedAggregations(String aggregations) {
    this.deduplicatedAggregations.add(aggregations);
  }

  public void setDeduplicatedAggregations(List<String> deduplicatedAggregations) {
    this.deduplicatedAggregations = deduplicatedAggregations;
  }

  public int[] getLevels() {
    return levels;
  }

  public void setLevels(int[] levels) {
    this.levels = levels;
  }

  public void setGroupByLevelController(GroupByLevelController groupByLevelController) {
    this.groupByLevelController = groupByLevelController;
  }

  public Map<String, AggregateResult> getGroupPathsResultMap() {
    return groupPathsResultMap;
  }

  public Map<String, AggregateResult> groupAggResultByLevel(
      List<AggregateResult> aggregateResults) {
    if (!groupPathsResultMap.isEmpty()) {
      groupPathsResultMap.clear();
    }
    for (int i = 0; i < getDeduplicatedPaths().size(); i++) {
      String rawPath = getRawPath(i);
      String transformedPath = groupByLevelController.getGroupedPath(rawPath);
      AggregateResult result = groupPathsResultMap.get(transformedPath);
      if (result == null) {
        groupPathsResultMap.put(transformedPath, aggregateResults.get(i));
      } else {
        result.merge(aggregateResults.get(i));
        groupPathsResultMap.put(transformedPath, result);
      }
    }
    return groupPathsResultMap;
  }

  public Map<AggregateResult, AggregateResult> getresultToGroupedAhead() {
    return resultToGroupedAhead;
  }

  public void groupAggResultByLevelBeforeAggregation(List<AggregateResult> aggregateResults) {
    List<TSDataType> seriesDataTypes = this.getDeduplicatedDataTypes();
    groupedPathToTSDataType.clear();
    groupPathsResultMap.clear();
    resultToGroupedAhead.clear();

    if (!this.isGroupByLevel()) {
      for (int i = 0; i < getDeduplicatedPaths().size(); i++) {
        resultToGroupedAhead.put(aggregateResults.get(i), aggregateResults.get(i));
      }
      return;
    }

    // find the biggest(Double>Float>INT64>INT32) TsDataType for grouped AggregationResult
    for (int i = 0; i < getDeduplicatedPaths().size(); i++)
      if (aggregateResults.get(i).groupByLevelBeforeAggregation()) {
        String rawPath = getRawPath(i);
        String transformedPath = groupByLevelController.getGroupedPath(rawPath);
        TSDataType dataType = groupedPathToTSDataType.get(transformedPath);
        if (dataType == null || dataType.serialize() < seriesDataTypes.get(i).serialize()) {
          groupedPathToTSDataType.put(transformedPath, seriesDataTypes.get(i));
        }
      }
    for (int i = 0; i < getDeduplicatedPaths().size(); i++)
      if (aggregateResults.get(i).groupByLevelBeforeAggregation()) {
        String rawPath = getRawPath(i);
        String transformedPath = groupByLevelController.getGroupedPath(rawPath);

        AggregateResult groupedResult = groupPathsResultMap.get(transformedPath);
        if (groupedResult == null) {
          groupedResult =
              AggregateResultFactory.getAggrResultByName(
                  deduplicatedAggregations.get(i),
                  groupedPathToTSDataType.get(transformedPath),
                  aggregateResults.get(i).isAscending());
          groupPathsResultMap.put(transformedPath, groupedResult);
        }
        resultToGroupedAhead.put(aggregateResults.get(i), groupedResult);
      } else {
        resultToGroupedAhead.put(aggregateResults.get(i), aggregateResults.get(i));
      }
  }

  public Map<String, AggregateResult> groupAggResultByLevelAfterAggregation(
      List<AggregateResult> aggregateResults) {
    for (int i = 0; i < getDeduplicatedPaths().size(); i++)
      if (!aggregateResults.get(i).groupByLevelBeforeAggregation()) {
        String rawPath = getRawPath(i);
        String transformedPath = groupByLevelController.getGroupedPath(rawPath);
        AggregateResult result = groupPathsResultMap.get(transformedPath);
        if (result == null) {
          groupPathsResultMap.put(transformedPath, aggregateResults.get(i));
        } else {
          result.merge(aggregateResults.get(i));
          groupPathsResultMap.put(transformedPath, result);
        }
      }
    return groupPathsResultMap;
  }

  public String getRawPath(int aggrIndex) {
    return String.format(
        "%s(%s)",
        deduplicatedAggregations.get(aggrIndex),
        getDeduplicatedPaths().get(aggrIndex).getFullPath());
  }

  @Override
  public boolean isGroupByLevel() {
    return levels != null;
  }

  @Override
  public String getColumnForReaderFromPath(PartialPath path, int pathIndex) {
    return resultColumns.get(pathIndex).getResultColumnName();
  }

  @Override
  public String getColumnForDisplay(String columnForReader, int pathIndex) {
    String columnForDisplay = columnForReader;
    if (isGroupByLevel()) {
      PartialPath path = paths.get(pathIndex);
      String functionName = aggregations.get(pathIndex);
      String aggregatePath =
          groupByLevelController.getGroupedPath(
              String.format("%s(%s)", functionName, path.getFullPath()));
      columnForDisplay = aggregatePath;
    }
    return columnForDisplay;
  }
}
