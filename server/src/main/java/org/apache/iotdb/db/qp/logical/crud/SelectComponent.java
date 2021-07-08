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

package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** this class maintains information from select clause. */
public final class SelectComponent {

  private final ZoneId zoneId;

  private boolean hasAggregationFunction = false;
  private boolean hasTimeSeriesGeneratingFunction = false;

  private List<ResultColumn> resultColumns = new ArrayList<>();

  private List<PartialPath> pathsCache;
  private List<String> aggregationFunctionsCache;

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectComponent(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public SelectComponent(SelectComponent selectComponent) {
    zoneId = selectComponent.zoneId;
    hasAggregationFunction = selectComponent.hasAggregationFunction;
    hasTimeSeriesGeneratingFunction = selectComponent.hasTimeSeriesGeneratingFunction;
    resultColumns.addAll(selectComponent.resultColumns);
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public boolean hasAggregationFunction() {
    return hasAggregationFunction;
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return hasTimeSeriesGeneratingFunction;
  }

  public void addResultColumn(ResultColumn resultColumn) {
    resultColumns.add(resultColumn);
    if (resultColumn.getExpression().isAggregationFunctionExpression()) {
      hasAggregationFunction = true;
    }
    if (resultColumn.getExpression().isTimeSeriesGeneratingFunctionExpression()) {
      hasTimeSeriesGeneratingFunction = true;
    }
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;

    pathsCache = null;
    aggregationFunctionsCache = null;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public List<PartialPath> getPaths() {
    if (pathsCache == null) {
      pathsCache = new ArrayList<>();
      for (ResultColumn resultColumn : resultColumns) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof TimeSeriesOperand) {
          pathsCache.add(((TimeSeriesOperand) expression).getPath());
        } else if (expression instanceof FunctionExpression
            && expression.isAggregationFunctionExpression()) {
          pathsCache.add(
              ((TimeSeriesOperand) ((FunctionExpression) expression).getExpressions().get(0))
                  .getPath());
        } else {
          pathsCache.add(null);
        }
      }
    }
    return pathsCache;
  }

  public List<String> getAggregationFunctions() {
    if (!hasAggregationFunction()) {
      return null;
    }
    if (aggregationFunctionsCache == null) {
      aggregationFunctionsCache = new ArrayList<>();
      for (ResultColumn resultColumn : resultColumns) {
        Expression expression = resultColumn.getExpression();
        aggregationFunctionsCache.add(
            expression instanceof FunctionExpression
                ? ((FunctionExpression) resultColumn.getExpression()).getFunctionName()
                : null);
      }
    }
    return aggregationFunctionsCache;
  }
}
