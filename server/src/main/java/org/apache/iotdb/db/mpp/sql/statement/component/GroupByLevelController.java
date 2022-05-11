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

package org.apache.iotdb.db.mpp.sql.statement.component;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.*;

/**
 * This class is used to control the row number of group by level query. For example, selected
 * series[root.sg.d1.s1, root.sg.d2.s1, root.sg2.d1.s1], level = 1; the result rows will be
 * [root.sg.*.s1, root.sg2.*.s1], sLimit and sOffset will be used to control the result numbers
 * rather than the selected series.
 */
public class GroupByLevelController {

  public static String ALIAS_ERROR_MESSAGE1 =
      "alias '%s' can only be matched with one result column";
  public static String ALIAS_ERROR_MESSAGE2 = "Result column %s with more than one alias[%s, %s]";
  private final int seriesLimit;
  private int seriesOffset;
  Set<String> limitPaths;
  Set<String> offsetPaths;
  private final int[] levels;
  int prevSize = 0;
  /** count(root.sg.d1.s1) with level = 1 -> count(root.*.d1.s1) */
  private Map<String, String> groupedPathMap;
  /** count(root.*.d1.s1) -> alias */
  private Map<String, String> columnToAliasMap;
  /**
   * Only used to check whether one alisa is corresponding to only one column. i.e. Different
   * columns can't have the same alias.
   */
  private Map<String, String> aliasToColumnMap;

  public GroupByLevelController(QueryStatement queryStatement) {
    this.seriesLimit = queryStatement.getSeriesLimit();
    this.seriesOffset = queryStatement.getSeriesOffset();
    this.limitPaths = seriesLimit > 0 ? new HashSet<>() : null;
    this.offsetPaths = seriesOffset > 0 ? new HashSet<>() : null;
    this.groupedPathMap = new LinkedHashMap<>();
    this.levels =
        ((AggregationQueryStatement) queryStatement).getGroupByLevelComponent().getLevels();
  }

  public String getGroupedPath(String rawPath) {
    return groupedPathMap.get(rawPath);
  }

  public String getAlias(String originName) {
    return columnToAliasMap != null && columnToAliasMap.get(originName) != null
        ? columnToAliasMap.get(originName)
        : null;
  }

  public void control(ResultColumn rawColumn, List<ResultColumn> resultColumns)
      throws StatementAnalyzeException {
    Set<Integer> countWildcardIterIndices = getCountStarIndices(rawColumn);

    // `resultColumns` includes all result columns after removing wildcards, so we need to skip
    // those we have processed
    Iterator<ResultColumn> iterator = resultColumns.iterator();
    for (int i = 0; i < prevSize; i++) {
      iterator.next();
    }

    while (iterator.hasNext()) {
      ResultColumn resultColumn = iterator.next();
      Expression rootExpression = resultColumn.getExpression();
      boolean hasAggregation = false;
      int idx = 0;
      for (Iterator<Expression> it = rootExpression.iterator(); it.hasNext(); ) {
        Expression expression = it.next();
        if (expression instanceof FunctionExpression
            && expression.isPlainAggregationFunctionExpression()) {
          hasAggregation = true;
          List<PartialPath> paths = ((FunctionExpression) expression).getPaths();
          String functionName = ((FunctionExpression) expression).getFunctionName();
          boolean isCountStar = countWildcardIterIndices.contains(idx++);
          String groupedPath =
              generatePartialPathByLevel(isCountStar, paths.get(0).getNodes(), levels);
          String rawPath = String.format("%s(%s)", functionName, paths.get(0).getFullPath());
          String pathWithFunction = String.format("%s(%s)", functionName, groupedPath);

          if (seriesLimit == 0 && seriesOffset == 0) {
            groupedPathMap.put(rawPath, pathWithFunction);
            checkAliasAndUpdateAliasMap(rawColumn, pathWithFunction);
          } else {
            // We cannot judge whether the path after grouping exists until we add it to set
            if (seriesOffset > 0 && offsetPaths != null) {
              offsetPaths.add(pathWithFunction);
              if (offsetPaths.size() <= seriesOffset) {
                iterator.remove();
                if (offsetPaths.size() == seriesOffset) {
                  seriesOffset = 0;
                }
              }
            } else if (offsetPaths == null || !offsetPaths.contains(pathWithFunction)) {
              limitPaths.add(pathWithFunction);
              if (seriesLimit > 0 && limitPaths.size() > seriesLimit) {
                iterator.remove();
                limitPaths.remove(pathWithFunction);
              } else {
                groupedPathMap.put(rawPath, pathWithFunction);
                checkAliasAndUpdateAliasMap(rawColumn, pathWithFunction);
              }
            } else {
              iterator.remove();
            }
          }
        }
      }
      if (!hasAggregation) {
        throw new StatementAnalyzeException(rootExpression + " can't be used in group by level.");
      }
    }
    prevSize = resultColumns.size();
  }

  // As one expression may have many aggregation results in the tree leaf, here we should traverse
  // all the successor expressions and record the count(*) indices
  private Set<Integer> getCountStarIndices(ResultColumn rawColumn) {
    Set<Integer> countWildcardIterIndices = new HashSet<>();
    int idx = 0;
    for (Iterator<Expression> it = rawColumn.getExpression().iterator(); it.hasNext(); ) {
      Expression expression = it.next();
      if (expression instanceof FunctionExpression
          && expression.isPlainAggregationFunctionExpression()
          && ((FunctionExpression) expression).isCountStar()) {
        countWildcardIterIndices.add(idx);
      }
      idx++;
    }
    return countWildcardIterIndices;
  }

  private void checkAliasAndUpdateAliasMap(ResultColumn rawColumn, String originName)
      throws StatementAnalyzeException {
    if (!rawColumn.hasAlias()) {
      return;
    } else if (columnToAliasMap == null) {
      columnToAliasMap = new HashMap<>();
      aliasToColumnMap = new HashMap<>();
    }
    // If an alias is corresponding to more than one result column, throw an exception
    if (columnToAliasMap.get(originName) == null) {
      if (aliasToColumnMap.get(rawColumn.getAlias()) != null) {
        throw new StatementAnalyzeException(
            String.format(ALIAS_ERROR_MESSAGE1, rawColumn.getAlias()));
      } else {
        columnToAliasMap.put(originName, rawColumn.getAlias());
        aliasToColumnMap.put(rawColumn.getAlias(), originName);
      }
      // If a result column is corresponding to more than one alias, throw an exception
    } else if (!columnToAliasMap.get(originName).equals(rawColumn.getAlias())) {
      throw new StatementAnalyzeException(
          String.format(
              ALIAS_ERROR_MESSAGE2,
              originName,
              columnToAliasMap.get(originName),
              rawColumn.getAlias()));
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
  public String generatePartialPathByLevel(boolean isCountStar, String[] nodes, int[] pathLevels) {
    Set<Integer> levelSet = new HashSet<>();
    for (int level : pathLevels) {
      levelSet.add(level);
    }

    StringBuilder transformedPath = new StringBuilder();
    transformedPath.append(nodes[0]).append(TsFileConstant.PATH_SEPARATOR);
    for (int k = 1; k < nodes.length - 1; k++) {
      if (levelSet.contains(k)) {
        transformedPath.append(nodes[k]);
      } else {
        transformedPath.append(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD);
      }
      transformedPath.append(TsFileConstant.PATH_SEPARATOR);
    }
    if (isCountStar) {
      transformedPath.append(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD);
    } else {
      transformedPath.append(nodes[nodes.length - 1]);
    }
    return transformedPath.toString();
  }
}
