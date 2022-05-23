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

package org.apache.iotdb.db.protocol.influxdb.function.aggregator;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.utils.MathUtils;

import java.util.ArrayList;
import java.util.List;

public class InfluxSumFunction extends InfluxAggregator {
  private final List<Double> numbers = new ArrayList<>();
  private long sum = 0;

  public InfluxSumFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    return new InfluxFunctionValue(numbers.size() == 0 ? numbers : MathUtils.sum(numbers), 0L);
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    return new InfluxFunctionValue(sum, 0L);
  }

  @Override
  public String getFunctionName() {
    return InfluxSQLConstant.SUM;
  }

  @Override
  public void updateValueBruteForce(InfluxFunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (!(value instanceof Number)) {
      throw new IllegalArgumentException("not support this type");
    }

    double tmpValue = ((Number) value).doubleValue();
    numbers.add(tmpValue);
  }

  @Override
  public void updateValueIoTDBFunc(InfluxFunctionValue... functionValues) {
    sum += (double) functionValues[0].getValue();
  }
}
