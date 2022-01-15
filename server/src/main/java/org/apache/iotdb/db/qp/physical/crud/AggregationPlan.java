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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.GroupByLevelController;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregationPlan extends RawDataQueryPlan {

  // e.g., for select count(s1), count(s1), count(s2), count(s2), sum (s1)
  // aggregations are count, count, count, count, sum
  // deduplicatedAggregations are count, count, sum

  private List<String> aggregations = new ArrayList<>();
  private List<String> deduplicatedAggregations = new ArrayList<>();
  private GroupByLevelController groupByLevelController;
  private boolean isCountStar;

  private int[] levels;
  // group by level aggregation result path
  private final Map<String, AggregateResult> levelAggPaths = new LinkedHashMap<>();

  public AggregationPlan() {
    super();
    setOperatorType(Operator.OperatorType.AGGREGATION);
  }

  /** @author Yuyuan Kang */
  @Override
  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
    for (int i = 0; i < aggregations.size(); i++) {
      if (aggregations.get(i).equals("min_value") || aggregations.get(i).equals("max_value")) {
        dataTypes.set(i, SchemaUtils.transformMinMaxDataType(dataTypes.get(i)));
      }
    }
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

  public boolean isGroupByLevel() {
    return !(levels == null || levels.length == 0);
  }

  public boolean isCountStar() {
    return isCountStar;
  }

  public void setCountStar(boolean countStar) {
    isCountStar = countStar;
  }

  public Map<String, AggregateResult> getAggPathByLevel() {
    if (!levelAggPaths.isEmpty()) {
      return levelAggPaths;
    }
    List<PartialPath> seriesPaths = getPaths();
    List<TSDataType> dataTypes = getDataTypes();
    for (int i = 0; i < seriesPaths.size(); i++) {
      String rawPath = getAggregations().get(i) + "(" + seriesPaths.get(i).getFullPath() + ")";
      String key = groupByLevelController.getGroupedPath(rawPath);
      int finalI = i;
      levelAggPaths.computeIfAbsent(
          key,
          k ->
              AggregateResultFactory.getAggrResultByName(
                  getAggregations().get(finalI), dataTypes.get(finalI)));
    }
    return levelAggPaths;
  }

  @Override
  public void setAlignByTime(boolean align) throws QueryProcessException {
    if (!align) {
      throw new QueryProcessException(
          getOperatorType().name() + " doesn't support disable align clause.");
    }
  }

  @Override
  public String getColumnForReaderFromPath(PartialPath path, int pathIndex) {
    String columnForReader = super.getColumnForReaderFromPath(path, pathIndex);
    if (!path.isTsAliasExists()) {
      columnForReader = this.getAggregations().get(pathIndex) + "(" + columnForReader + ")";
    }
    return columnForReader;
  }

  @Override
  public String getColumnForDisplay(String columnForReader, int pathIndex) {
    String columnForDisplay = columnForReader;
    if (isGroupByLevel()) {
      columnForDisplay = groupByLevelController.getGroupedPath(columnForDisplay);
    }
    return columnForDisplay;
  }

  public GroupByLevelController getGroupByLevelController() {
    return groupByLevelController;
  }

  public void setGroupByLevelController(GroupByLevelController groupByLevelController) {
    this.groupByLevelController = groupByLevelController;
  }
}
