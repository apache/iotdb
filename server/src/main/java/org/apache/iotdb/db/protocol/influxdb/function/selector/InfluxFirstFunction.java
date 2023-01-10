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

package org.apache.iotdb.db.protocol.influxdb.function.selector;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSqlConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;

import java.util.List;

public class InfluxFirstFunction extends InfluxSelector {
  private Object value;

  public InfluxFirstFunction(List<Expression> expressionList) {
    super(expressionList);
    this.setTimestamp(Long.MAX_VALUE);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    return new InfluxFunctionValue(value, this.getTimestamp());
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    return calculateBruteForce();
  }

  @Override
  public void updateValueIoTDBFunc(InfluxFunctionValue... functionValues) {
    if (functionValues[0].getTimestamp() < getTimestamp()) {
      value = functionValues[0].getValue();
      setTimestamp(functionValues[0].getTimestamp());
    }
  }

  @Override
  public void updateValueAndRelateValues(
      InfluxFunctionValue functionValue, List<Object> relatedValues) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (timestamp <= this.getTimestamp()) {
      this.value = value;
      this.setTimestamp(timestamp);
      this.setRelatedValues(relatedValues);
    }
  }

  @Override
  public String getFunctionName() {
    return InfluxSqlConstant.FIRST;
  }
}
