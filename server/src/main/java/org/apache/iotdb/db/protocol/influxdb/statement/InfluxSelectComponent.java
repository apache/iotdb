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

package org.apache.iotdb.db.protocol.influxdb.statement;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSqlConstant;

/** this class maintains information from select clause. */
public final class InfluxSelectComponent extends SelectComponent {

  private boolean hasAggregationFunction = false;
  private boolean hasSelectorFunction = false;
  private boolean hasMoreSelectorFunction = false;
  private boolean hasMoreFunction = false;
  private boolean hasFunction = false;
  private boolean hasCommonQuery = false;
  private boolean hasOnlyTraverseFunction = false;

  public InfluxSelectComponent() {
    super(null);
  }

  @Override
  public void addResultColumn(ResultColumn resultColumn) {
    Expression expression = resultColumn.getExpression();
    if (expression instanceof FunctionExpression) {
      String functionName = ((FunctionExpression) expression).getFunctionName();
      if (InfluxSqlConstant.getNativeFunctionNames().contains(functionName.toLowerCase())) {
        if (hasFunction) {
          hasMoreFunction = true;
        } else {
          hasFunction = true;
        }
      }
      if (InfluxSqlConstant.getNativeSelectorFunctionNames().contains(functionName.toLowerCase())) {
        if (hasSelectorFunction) {
          hasMoreSelectorFunction = true;
        } else {
          hasSelectorFunction = true;
        }
      } else {
        hasAggregationFunction = true;
      }
      if (InfluxSqlConstant.getOnlyTraverseFunctionNames().contains(functionName.toLowerCase())) {
        hasOnlyTraverseFunction = true;
      }
    }
    if (expression instanceof TimeSeriesOperand) {
      hasCommonQuery = true;
    }
    resultColumns.add(resultColumn);
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

  public boolean isHasOnlyTraverseFunction() {
    return hasOnlyTraverseFunction;
  }

  public boolean isHasMoreSelectorFunction() {
    return hasMoreSelectorFunction;
  }

  public boolean isHasFunction() {
    return hasFunction;
  }
}
