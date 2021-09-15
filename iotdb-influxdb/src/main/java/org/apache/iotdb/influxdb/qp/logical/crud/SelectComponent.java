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

package org.apache.iotdb.influxdb.qp.logical.crud;

import org.apache.iotdb.influxdb.qp.constant.SQLConstant;
import org.apache.iotdb.influxdb.query.expression.Expression;
import org.apache.iotdb.influxdb.query.expression.ResultColumn;
import org.apache.iotdb.influxdb.query.expression.unary.FunctionExpression;
import org.apache.iotdb.influxdb.query.expression.unary.NodeExpression;

import java.util.ArrayList;
import java.util.List;

/** this class maintains information from select clause. */
public final class SelectComponent {

  private List<ResultColumn> resultColumns = new ArrayList<>();

  private boolean hasAggregationFunction = false;
  private boolean hasSelectorFunction = false;
  private boolean hasMoreSelectorFunction = false;
  private boolean hasMoreFunction = false;
  private boolean hasFunction = false;
  private boolean hasCommonQuery = false;

  public void addResultColumn(ResultColumn resultColumn) {
    Expression expression = resultColumn.getExpression();
    if (expression instanceof FunctionExpression) {
      String functionName = ((FunctionExpression) expression).getFunctionName();
      if (SQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase())) {
        if (hasFunction) {
          hasMoreFunction = true;
        } else {
          hasFunction = true;
        }
      }
      if (SQLConstant.getNativeSelectorFunctionNames().contains(functionName.toLowerCase())) {
        if (hasSelectorFunction) {
          hasMoreSelectorFunction = true;
        } else {
          hasSelectorFunction = true;
        }
      } else {
        hasAggregationFunction = true;
      }
    }
    if (expression instanceof NodeExpression) {
      hasCommonQuery = true;
    }
    resultColumns.add(resultColumn);
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public boolean isHasAggregationFunction() {
    return hasAggregationFunction;
  }

  public boolean isHasMoreFunction() {
    return hasMoreFunction;
  }

  public boolean isHasCommonQuery() {
    return hasCommonQuery;
  }

  public boolean isHasSelectorFunction() {
    return hasSelectorFunction;
  }

  public boolean isHasMoreSelectorFunction() {
    return hasMoreSelectorFunction;
  }

  public boolean isHasFunction() {
    return hasFunction;
  }
}
