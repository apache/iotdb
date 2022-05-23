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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to control the row number of group by level query. For example, selected
 * series[root.sg.d1.s1, root.sg.d2.s1, root.sg2.d1.s1], level = 1; the result rows will be
 * [root.sg.*.s1, root.sg2.*.s1], sLimit and sOffset will be used to control the result numbers
 * rather than the selected series.
 */
public class GroupByLevelController {

  private final int[] levels;

  /** count(root.sg.d1.s1) with level = 1 -> count(root.*.d1.s1) */
  private final Map<Expression, Set<Expression>> groupedPathMap;

  /** count(root.*.d1.s1) -> alias */
  private final Map<String, String> columnToAliasMap;

  /**
   * Only used to check whether one alisa is corresponding to only one column. i.e. Different
   * columns can't have the same alias.
   */
  private final Map<String, String> aliasToColumnMap;

  public GroupByLevelController(int[] levels) {
    this.levels = levels;
    this.groupedPathMap = new LinkedHashMap<>();
    this.columnToAliasMap = new HashMap<>();
    this.aliasToColumnMap = new HashMap<>();
  }

  public void control(Expression expression, String alias) {
    if (!(expression instanceof FunctionExpression
        && expression.isBuiltInAggregationFunctionExpression())) {
      throw new SemanticException(expression + " can't be used in group by level.");
    }

    PartialPath rawPath = ((TimeSeriesOperand) expression.getExpressions().get(0)).getPath();
    PartialPath groupedPath = generatePartialPathByLevel(rawPath.getNodes(), levels);
    Expression groupedExpression =
        new FunctionExpression(
            ((FunctionExpression) expression).getFunctionName(),
            ((FunctionExpression) expression).getFunctionAttributes(),
            Collections.singletonList(new TimeSeriesOperand(groupedPath)));
    groupedPathMap.computeIfAbsent(groupedExpression, key -> new HashSet<>()).add(expression);

    if (alias != null) {
      checkAliasAndUpdateAliasMap(alias, groupedExpression.getExpressionString());
    }
  }

  private void checkAliasAndUpdateAliasMap(String alias, String groupedExpressionString)
      throws StatementAnalyzeException {
    // If an alias is corresponding to more than one result column, throw an exception
    if (columnToAliasMap.get(groupedExpressionString) == null) {
      if (aliasToColumnMap.get(alias) != null) {
        throw new StatementAnalyzeException(
            String.format("alias '%s' can only be matched with one result column", alias));
      } else {
        columnToAliasMap.put(groupedExpressionString, alias);
        aliasToColumnMap.put(alias, groupedExpressionString);
      }
      // If a result column is corresponding to more than one alias, throw an exception
    } else if (!columnToAliasMap.get(groupedExpressionString).equals(alias)) {
      throw new StatementAnalyzeException(
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
  public PartialPath generatePartialPathByLevel(String[] nodes, int[] pathLevels) {
    Set<Integer> levelSet = new HashSet<>();
    for (int level : pathLevels) {
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
    transformedNodes.add(nodes[nodes.length - 1]);
    return new PartialPath(transformedNodes.toArray(new String[0]));
  }

  public Map<Expression, Set<Expression>> getGroupedPathMap() {
    return groupedPathMap;
  }

  public String getAlias(String columnName) {
    return columnToAliasMap.get(columnName) != null ? columnToAliasMap.get(columnName) : null;
  }
}
