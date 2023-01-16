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
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregationPlan extends RawDataQueryPlan {

  // e.g., for select count(s1), count(s1), count(s2), count(s2), sum (s1)
  // aggregations are count, count, count, count, sum
  // deduplicatedAggregations are count, count, sum

  private List<String> aggregations = new ArrayList<>();
  private List<String> deduplicatedAggregations = new ArrayList<>();
  private List<Map<String, String>> parameters = new ArrayList<>();

  private int[] levels;
  private GroupByLevelController groupByLevelController;
  // group by level aggregation result path
  private final Map<String, AggregateResult> groupPathsResultMap = new LinkedHashMap<>();

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

  public List<Map<String, String>> getParameters() {
    return parameters;
  }

  public void setParameters(List<Map<String, String>> parameters) {
    this.parameters = parameters;
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
      String rawPath =
          String.format(
              "%s(%s)",
              deduplicatedAggregations.get(i), getDeduplicatedPaths().get(i).getFullPath());
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
