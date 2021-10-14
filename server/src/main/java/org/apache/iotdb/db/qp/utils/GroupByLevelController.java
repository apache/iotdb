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

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.utils.AggregateUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class GroupByLevelController {

  int seriesLimit, seriesOffset;
  Set<String> limitPaths;
  Set<String> offsetPaths;
  int[] levels;

  public GroupByLevelController(QueryOperator operator) {
    this.seriesLimit = operator.getSpecialClauseComponent().getSeriesLimit();
    this.seriesOffset = operator.getSpecialClauseComponent().getSeriesOffset();
    this.limitPaths = seriesLimit > 0 ? new HashSet<>() : null;
    this.offsetPaths = seriesOffset > 0 ? new HashSet<>() : null;
    this.levels = operator.getLevels();
  }

  public void control(List<ResultColumn> resultColumns) throws LogicalOptimizeException {
    Iterator<ResultColumn> iterator = resultColumns.iterator();
    while (iterator.hasNext()) {
      ResultColumn resultColumn = iterator.next();
      Expression expression = resultColumn.getExpression();
      if (expression instanceof FunctionExpression
          && expression.isAggregationFunctionExpression()) {
        List<PartialPath> paths = ((FunctionExpression) expression).getPaths();
        String functionName = ((FunctionExpression) expression).getFunctionName();
        String groupedPath =
            AggregateUtils.generatePartialPathByLevel(paths.get(0).getNodes(), levels);
        String pathWithFunction = String.format("%s(%s)", functionName, groupedPath);

        if (seriesOffset > 0) {
          offsetPaths.add(pathWithFunction);
          if (offsetPaths.size() <= seriesOffset) {
            iterator.remove();
          } else {
            seriesOffset = 0;
          }
        } else if (seriesLimit > 0) {
          if (offsetPaths == null || !offsetPaths.contains(pathWithFunction)) {
            limitPaths.add(pathWithFunction);
            if (limitPaths.size() > seriesLimit) {
              iterator.remove();
              limitPaths.remove(pathWithFunction);
            }
          }
        }
      } else {
        throw new LogicalOptimizeException(
            expression.toString() + "can't be used in group by level.");
      }
    }
  }
}
