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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * This class is used to control the row number of group by level query. For example, selected
 * series[root.sg.d1.s1, root.sg.d2.s1, root.sg2.d1.s1], level = 1; the result rows will be
 * [root.sg.*.s1, root.sg2.*.s1], sLimit and sOffset will be used to control the result numbers
 * rather than the selected series.
 */
public class GroupByLevelController {

  private final int[] levels;

  /** count(root.sg.d1.s1) with level = 1 -> { count(root.*.d1.s1) : count(root.sg.d1.s1) } */
  private final Map<Expression, Set<Expression>> groupedExpressionToRawExpressionsMap;

  /** count(root.sg.d1.s1) with level = 1 -> { root.sg.d1.s1 : root.sg.*.s1 } */
  private final RawPathToGroupedPathMap rawPathToGroupedPathMap;

  /** count(root.*.d1.s1) -> alias */
  private final Map<String, String> columnToAliasMap;

  /**
   * Only used to check whether one alisa is corresponding to only one column. i.e. Different
   * columns can't have the same alias.
   */
  private final Map<String, String> aliasToColumnMap;

  public GroupByLevelController(int[] levels) {
    this.levels = levels;
    this.groupedExpressionToRawExpressionsMap = new LinkedHashMap<>();
    this.rawPathToGroupedPathMap = new RawPathToGroupedPathMap();
    this.columnToAliasMap = new HashMap<>();
    this.aliasToColumnMap = new HashMap<>();
  }

  public Expression control(Expression expression) {
    return control(false, expression, null);
  }

  public Expression control(boolean isCountStar, Expression expression, String alias) {
    // update rawPathToGroupedPathMap
    List<PartialPath> rawPaths =
        ExpressionAnalyzer.searchSourceExpressions(expression).stream()
            .map(timeSeriesOperand -> ((TimeSeriesOperand) timeSeriesOperand).getPath())
            .collect(Collectors.toList());
    for (PartialPath rawPath : rawPaths) {
      if (!rawPathToGroupedPathMap.containsKey(rawPath)) {
        rawPathToGroupedPathMap.put(rawPath, generatePartialPathByLevel(isCountStar, rawPath));
      }
    }

    // update groupedExpressionToRawExpressionsMap
    Set<Expression> rawAggregationExpressions =
        new HashSet<>(ExpressionAnalyzer.searchAggregationExpressions(expression));
    for (Expression rawAggregationExpression : rawAggregationExpressions) {
      Expression groupedExpression =
          ExpressionAnalyzer.replaceRawPathWithGroupedPath(
              rawAggregationExpression, rawPathToGroupedPathMap);
      groupedExpressionToRawExpressionsMap
          .computeIfAbsent(groupedExpression, key -> new HashSet<>())
          .add(rawAggregationExpression);
    }

    // reconstruct input expression
    Expression groupedExpression =
        ExpressionAnalyzer.replaceRawPathWithGroupedPath(expression, rawPathToGroupedPathMap);
    if (alias != null) {
      checkAliasAndUpdateAliasMap(alias, groupedExpression.getExpressionString());
    }
    return groupedExpression;
  }

  private void checkAliasAndUpdateAliasMap(String alias, String groupedExpressionString) {
    // If an alias is corresponding to more than one result column, throw an exception
    if (columnToAliasMap.get(groupedExpressionString) == null) {
      if (aliasToColumnMap.get(alias) != null) {
        throw new SemanticException(
            String.format("alias '%s' can only be matched with one result column", alias));
      } else {
        columnToAliasMap.put(groupedExpressionString, alias);
        aliasToColumnMap.put(alias, groupedExpressionString);
      }
      // If a result column is corresponding to more than one alias, throw an exception
    } else if (!columnToAliasMap.get(groupedExpressionString).equals(alias)) {
      throw new SemanticException(
          String.format(
              "Result column %s with more than one alias[%s, %s]",
              groupedExpressionString, columnToAliasMap.get(groupedExpressionString), alias));
    }
  }

  /**
   * Transform an originalPath to a partial path that satisfies given level. Path nodes don't
   * satisfy the given level will be replaced by "*" except the sensor level, e.g.
   * generatePartialPathByLevel("root.sg.dh.d1.s1", 2) will return "root.*.dh.*.s1".
   *
   * <p>Especially, if count(*), then the sensor level will be replaced by "*" too.
   *
   * @return result partial path
   */
  public PartialPath generatePartialPathByLevel(boolean isCountStar, PartialPath rawPath) {
    String[] nodes = rawPath.getNodes();
    Set<Integer> levelSet = new HashSet<>();
    for (int level : levels) {
      levelSet.add(level);
    }

    List<String> transformedNodes = new ArrayList<>(nodes.length);

    transformedNodes.add(nodes[0]);
    for (int k = 1; k < nodes.length - 1; k++) {
      if (levelSet.contains(k)) {
        transformedNodes.add(nodes[k]);
      } else {
        transformedNodes.add(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD);
      }
    }
    if (isCountStar) {
      transformedNodes.add(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD);
    } else {
      transformedNodes.add(nodes[nodes.length - 1]);
    }

    // GroupByLevelNode can only accept intermediate input, so it doesn't matter what the origin
    // series datatype is for aggregation like COUNT, SUM. But for MAX_VALUE, it must be consistent
    // across different time series. And we will take one as the final type of grouped series.
    MeasurementPath groupedPath =
        new MeasurementPath(
            new PartialPath(transformedNodes.toArray(new String[0])),
            ((MeasurementPath) rawPath).getMeasurementSchema());
    if (rawPath.isMeasurementAliasExists()) {
      groupedPath.setMeasurementAlias(rawPath.getMeasurementAlias());
    }
    return groupedPath;
  }

  public Map<Expression, Set<Expression>> getGroupedExpressionToRawExpressionsMap() {
    return groupedExpressionToRawExpressionsMap;
  }

  public String getAlias(String columnName) {
    return columnToAliasMap.get(columnName) != null ? columnToAliasMap.get(columnName) : null;
  }

  public static class RawPathToGroupedPathMap {

    // key - a pair of raw path and its measurement alias
    // value - grouped path
    private final Map<Pair<PartialPath, String>, PartialPath> map = new HashMap<>();

    public RawPathToGroupedPathMap() {
      // do nothing
    }

    public boolean containsKey(PartialPath rawPath) {
      return map.containsKey(new Pair<>(rawPath, rawPath.getMeasurementAlias()));
    }

    public void put(PartialPath rawPath, PartialPath groupedPath) {
      map.put(new Pair<>(rawPath, rawPath.getMeasurementAlias()), groupedPath);
    }

    public PartialPath get(PartialPath rawPath) {
      PartialPath groupedPath = map.get(new Pair<>(rawPath, rawPath.getMeasurementAlias()));
      checkState(
          groupedPath != null, "path '%s' is not analyzed in GroupByLevelController.", rawPath);
      return groupedPath;
    }
  }
}
