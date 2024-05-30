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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeExpressionType;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;

public class GroupByLevelHelper {

  private final int[] levels;

  /** count(root.sg.d1.s1) with level = 1 -> { count(root.*.d1.s1) : count(root.sg.d1.s1) } */
  private final Map<Expression, Set<Expression>> groupedAggregationExpressionToRawExpressionsMap;

  /** count(root.sg.d1.s1) with level = 1 -> { root.sg.d1.s1 : root.sg.*.s1 } */
  private final RawPathToGroupedPathMap rawPathToGroupedPathMap;

  /** count(root.*.d1.s1) -> alias */
  private final Map<String, String> columnToAliasMap;

  /**
   * Only used to check whether one alisa is corresponding to only one column. i.e. Different
   * columns can't have the same alias.
   */
  private final Map<String, String> aliasToColumnMap;

  private final Map<Expression, Set<Expression>> groupByLevelExpressions;

  public GroupByLevelHelper(int[] levels) {
    this.levels = levels;
    this.groupedAggregationExpressionToRawExpressionsMap = new HashMap<>();
    this.rawPathToGroupedPathMap = new RawPathToGroupedPathMap();
    this.columnToAliasMap = new HashMap<>();
    this.aliasToColumnMap = new HashMap<>();
    this.groupByLevelExpressions = new LinkedHashMap<>();
  }

  public Expression applyLevels(Expression expression, Analysis analysis) {
    return applyLevels(false, expression, null, analysis);
  }

  public Expression applyLevels(
      boolean isCountStar, Expression expression, String alias, Analysis analysis) {
    analyzeExpressionType(analysis, expression);
    Expression outputExpression = ExpressionAnalyzer.replaceSubTreeWithView(expression, analysis);

    // construct output expression
    // e.g. count(root.sg.d1.s1) -> count(root.sg.*.s1)
    Expression groupedOutputExpression =
        ExpressionAnalyzer.replaceRawPathWithGroupedPath(
            outputExpression,
            rawPathToGroupedPathMap,
            path -> transformPathByLevels(isCountStar, path));
    if (alias != null) {
      checkAliasAndUpdateAliasMap(alias, groupedOutputExpression.toString());
    }

    // update groupedAggregationExpressionToRawExpressionsMap
    Set<Expression> rawAggregationExpressions =
        new HashSet<>(ExpressionAnalyzer.searchAggregationExpressions(expression));
    for (Expression rawAggregationExpression : rawAggregationExpressions) {
      Expression rawOutputAggregationExpression =
          ExpressionAnalyzer.replaceSubTreeWithView(rawAggregationExpression, analysis);
      Expression groupedOutputAggregationExpression =
          ExpressionAnalyzer.replaceRawPathWithGroupedPath(
              rawOutputAggregationExpression,
              rawPathToGroupedPathMap,
              path -> transformPathByLevels(isCountStar, path));
      groupedOutputAggregationExpression = normalizeExpression(groupedOutputAggregationExpression);
      analyzeExpressionType(analysis, groupedOutputAggregationExpression);

      rawAggregationExpression =
          ExpressionAnalyzer.normalizeExpression(rawAggregationExpression, false);
      analyzeExpressionType(analysis, rawAggregationExpression);

      groupedAggregationExpressionToRawExpressionsMap
          .computeIfAbsent(groupedOutputAggregationExpression, key -> new HashSet<>())
          .add(rawAggregationExpression);
    }
    return groupedOutputExpression;
  }

  public void updateGroupByLevelExpressions(Expression groupedExpression) {
    for (Expression groupedAggregationExpression :
        searchAggregationExpressions(groupedExpression)) {
      Set<Expression> groupedExpressionSet =
          groupedAggregationExpressionToRawExpressionsMap.get(groupedAggregationExpression);
      groupByLevelExpressions
          .computeIfAbsent(groupedAggregationExpression, key -> new HashSet<>())
          .addAll(groupedExpressionSet);
    }
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
   * transformPathByLevels("root.sg.dh.d1.s1", 2) will return "root.*.dh.*.s1".
   *
   * <p>Especially, if count(*), then the sensor level will be replaced by "*" too.
   *
   * @return result partial path
   */
  private PartialPath transformPathByLevels(boolean isCountStar, PartialPath rawPath) {
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

  public Map<Expression, Set<Expression>> getGroupByLevelExpressions() {
    return groupByLevelExpressions;
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
      checkState(groupedPath != null, "path '%s' is not analyzed in GroupByLevelHelper.", rawPath);
      return groupedPath;
    }
  }
}
