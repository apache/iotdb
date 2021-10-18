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

package org.apache.iotdb.db.qp.utils;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.HashSet;
import java.util.Iterator;
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

  private final int seriesLimit;
  private int seriesOffset;
  Set<String> limitPaths;
  Set<String> offsetPaths;
  private final int[] levels;
  int prevSize = 0;
  /** count(root.sg.d1.s1) with level = 1 -> count(root.*.d1.s1) */
  private Map<String, String> groupedPathMap;

  public GroupByLevelController(QueryOperator operator) {
    this.seriesLimit = operator.getSpecialClauseComponent().getSeriesLimit();
    this.seriesOffset = operator.getSpecialClauseComponent().getSeriesOffset();
    this.limitPaths = seriesLimit > 0 ? new HashSet<>() : null;
    this.offsetPaths = seriesOffset > 0 ? new HashSet<>() : null;
    this.groupedPathMap = new LinkedHashMap<>();
    this.levels = operator.getLevels();
  }

  public String getGroupedPath(String rawPath) {
    return groupedPathMap.get(rawPath);
  }

  public void control(ResultColumn rawColumn, List<ResultColumn> resultColumns)
      throws LogicalOptimizeException {
    boolean isCountStar = ((FunctionExpression) rawColumn.getExpression()).isCountStar();
    Iterator<ResultColumn> iterator = resultColumns.iterator();
    for (int i = 0; i < prevSize; i++) {
      iterator.next();
    }
    while (iterator.hasNext()) {
      ResultColumn resultColumn = iterator.next();
      Expression expression = resultColumn.getExpression();
      if (expression instanceof FunctionExpression
          && expression.isAggregationFunctionExpression()) {
        List<PartialPath> paths = ((FunctionExpression) expression).getPaths();
        String functionName = ((FunctionExpression) expression).getFunctionName();
        String groupedPath =
            generatePartialPathByLevel(isCountStar, paths.get(0).getNodes(), levels);
        String rawPath = String.format("%s(%s)", functionName, paths.get(0).getExactFullPath());
        String pathWithFunction = String.format("%s(%s)", functionName, groupedPath);

        if (seriesLimit == 0 && seriesOffset == 0) {
          groupedPathMap.put(rawPath, pathWithFunction);
        } else {
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
            }
          } else {
            iterator.remove();
          }
        }
      } else {
        throw new LogicalOptimizeException(
            expression.toString() + "can't be used in group by level.");
      }
    }
    prevSize = resultColumns.size();
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
