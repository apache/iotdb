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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;

import java.util.List;

public abstract class InfluxFunction {

  // contain possible parameters
  private List<Expression> expressionList;

  protected InfluxFunction(List<Expression> expressionList) {
    this.expressionList = expressionList;
  }

  protected InfluxFunction() {}

  public List<Expression> getExpressions() {
    return this.expressionList;
  }

  public String getParmaName() {
    if (expressionList == null) {
      throw new IllegalArgumentException("not support param");
    }
    TimeSeriesOperand parmaExpression = (TimeSeriesOperand) expressionList.get(0);
    return parmaExpression.getPath().getFullPath();
  }

  public abstract String getFunctionName();

  // calculate result by brute force
  public abstract InfluxFunctionValue calculateBruteForce();

  // calculate result by iotdb func
  public abstract InfluxFunctionValue calculateByIoTDBFunc();

  public abstract void updateValueIoTDBFunc(InfluxFunctionValue... functionValues);
}
