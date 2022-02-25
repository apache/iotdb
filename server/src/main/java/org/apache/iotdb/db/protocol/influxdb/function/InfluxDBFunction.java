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
package org.apache.iotdb.db.protocol.influxdb.function;

import org.apache.iotdb.db.protocol.influxdb.expression.unary.InfluxNodeExpression;
import org.apache.iotdb.db.query.expression.Expression;

import java.util.List;

public abstract class InfluxDBFunction {

  protected String path;
  // contain possible parameters
  private List<Expression> expressionList;

  public InfluxDBFunction(List<Expression> expressionList) {
    this.expressionList = expressionList;
  }

  public InfluxDBFunction(List<Expression> expressionList, String path) {
    this.expressionList = expressionList;
    this.path = path;
  }

  public InfluxDBFunction() {}

  // calculate result
  public abstract InfluxDBFunctionValue calculate();

  public abstract InfluxDBFunctionValue calculateByIoTDBFunc();

  public List<Expression> getExpressions() {
    return this.expressionList;
  }

  public String getParmaName() {
    if (expressionList == null) {
      throw new IllegalArgumentException("not support param");
    }
    InfluxNodeExpression parmaExpression = (InfluxNodeExpression) expressionList.get(0);
    return parmaExpression.getName();
  }
}
