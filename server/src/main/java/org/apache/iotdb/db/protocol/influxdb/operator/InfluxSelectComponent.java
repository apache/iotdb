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

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.expression.InfluxResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.InfluxFunctionExpression;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.InfluxNodeExpression;
import org.apache.iotdb.db.query.expression.Expression;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** this class maintains information from select clause. */
public final class InfluxSelectComponent
    extends org.apache.iotdb.db.qp.logical.crud.SelectComponent {

  private List<InfluxResultColumn> influxInfluxResultColumns = new ArrayList<>();

  private boolean hasAggregationFunction = false;
  private boolean hasSelectorFunction = false;
  private boolean hasMoreSelectorFunction = false;
  private boolean hasMoreFunction = false;
  private boolean hasFunction = false;
  private boolean hasCommonQuery = false;

  public InfluxSelectComponent() {
    super((ZoneId) null);
  }

  public void addResultColumn(InfluxResultColumn influxResultColumn) {
    Expression expression = influxResultColumn.getExpression();
    if (expression instanceof InfluxFunctionExpression) {
      String functionName = ((InfluxFunctionExpression) expression).getFunctionName();
      if (InfluxSQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase())) {
        if (hasFunction) {
          hasMoreFunction = true;
        } else {
          hasFunction = true;
        }
      }
      if (InfluxSQLConstant.getNativeSelectorFunctionNames().contains(functionName.toLowerCase())) {
        if (hasSelectorFunction) {
          hasMoreSelectorFunction = true;
        } else {
          hasSelectorFunction = true;
        }
      } else {
        hasAggregationFunction = true;
      }
    }
    if (expression instanceof InfluxNodeExpression) {
      hasCommonQuery = true;
    }
    influxInfluxResultColumns.add(influxResultColumn);
  }

  public List<InfluxResultColumn> getInfluxResultColumns() {
    return influxInfluxResultColumns;
  }

  public void setInfluxResultColumns(List<InfluxResultColumn> influxResultColumns) {
    this.influxInfluxResultColumns = influxResultColumns;
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
