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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** this class maintains information from select clause. */
public final class SelectComponent {

  private final ZoneId zoneId;

  private boolean hasPlainAggregationFunction = false;
  private boolean hasTimeSeriesGeneratingFunction = false;
  private boolean hasUserDefinedAggregationFunction = false;

  private List<ResultColumn> resultColumns = new ArrayList<>();

  private List<PartialPath> pathsCache;
  private List<String> aggregationFunctionsCache;

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectComponent(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public SelectComponent(SelectComponent selectComponent) {
    zoneId = selectComponent.zoneId;
    hasPlainAggregationFunction = selectComponent.hasPlainAggregationFunction;
    hasTimeSeriesGeneratingFunction = selectComponent.hasTimeSeriesGeneratingFunction;
    resultColumns.addAll(selectComponent.resultColumns);
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setHasPlainAggregationFunction(boolean hasPlainAggregationFunction) {
    this.hasPlainAggregationFunction = hasPlainAggregationFunction;
  }

  public boolean hasPlainAggregationFunction() {
    return hasPlainAggregationFunction;
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return hasTimeSeriesGeneratingFunction;
  }

  public boolean hasUserDefinedAggregationFunction() {
    return hasUserDefinedAggregationFunction;
  }

  public void addResultColumn(ResultColumn resultColumn) {
    resultColumns.add(resultColumn);
    if (resultColumn.getExpression().isUserDefinedAggregationFunctionExpression()) {
      hasUserDefinedAggregationFunction = true;
    }
    if (resultColumn.getExpression().isPlainAggregationFunctionExpression()) {
      hasPlainAggregationFunction = true;
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
            && expression.isPlainAggregationFunctionExpression()) {
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
    if (aggregationFunctionsCache == null) {
      aggregationFunctionsCache = new ArrayList<>();
      for (ResultColumn resultColumn : resultColumns) {
        Expression expression = resultColumn.getExpression();
        aggregationFunctionsCache.add(
            expression instanceof FunctionExpression
                ? ((FunctionExpression) resultColumn.getExpression()).getFunctionName()
                : null);
        // TODO: resultColumn.getExpression().getFunctionAttributes()(
      }
    }
    return aggregationFunctionsCache;
  }

  public List<Map<String, String>> getAggregationAttributes() {
    List<Map<String, String>> aggregationAttributesCache = new ArrayList<>();
    for (ResultColumn resultColumn : resultColumns) {
      Expression expression = resultColumn.getExpression();
      aggregationAttributesCache.add(
          expression instanceof FunctionExpression
              ? ((FunctionExpression) resultColumn.getExpression()).getFunctionAttributes()
              : null);
    }
    return aggregationAttributesCache;
  }
}
