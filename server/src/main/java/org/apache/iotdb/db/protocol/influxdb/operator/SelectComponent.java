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
package org.apache.iotdb.db.protocol.influxdb.operator;

import org.apache.iotdb.db.protocol.influxdb.constant.SQLConstant;
import org.apache.iotdb.db.protocol.influxdb.expression.ResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.FunctionExpression;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.NodeExpression;
import org.apache.iotdb.db.query.expression.Expression;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** this class maintains information from select clause. */
public final class SelectComponent extends org.apache.iotdb.db.qp.logical.crud.SelectComponent {

  private List<ResultColumn> influxResultColumns = new ArrayList<>();

  private boolean hasAggregationFunction = false;
  private boolean hasSelectorFunction = false;
  private boolean hasMoreSelectorFunction = false;
  private boolean hasMoreFunction = false;
  private boolean hasFunction = false;
  private boolean hasCommonQuery = false;

  public SelectComponent() {
    super((ZoneId) null);
  }

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
    influxResultColumns.add(resultColumn);
  }

  public List<ResultColumn> getInfluxResultColumns() {
    return influxResultColumns;
  }

  public void setInfluxResultColumns(List<ResultColumn> resultColumns) {
    this.influxResultColumns = resultColumns;
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
