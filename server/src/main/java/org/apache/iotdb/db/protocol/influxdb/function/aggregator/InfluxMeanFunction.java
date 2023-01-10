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
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSqlConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.utils.MathUtils;

import java.util.ArrayList;
import java.util.List;

public class InfluxMeanFunction extends InfluxAggregator {
  private final List<Double> numbers = new ArrayList<>();
  double sum = 0;
  long count = 0;

  public InfluxMeanFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    return new InfluxFunctionValue(numbers.isEmpty() ? numbers : MathUtils.mean(numbers), 0L);
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    return new InfluxFunctionValue(count != 0 ? sum / count : null, count != 0 ? 0L : null);
  }

  @Override
  public String getFunctionName() {
    return InfluxSqlConstant.MEAN;
  }

  @Override
  public void updateValueBruteForce(InfluxFunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (value instanceof Number) {
      numbers.add(((Number) value).doubleValue());
    } else {
      throw new IllegalArgumentException("mean is not a valid type");
    }
  }

  @Override
  public void updateValueIoTDBFunc(InfluxFunctionValue... functionValues) {
    if (functionValues.length == 1) {
      count += (long) functionValues[0].getValue();
    } else if (functionValues.length == 2) {
      sum += (double) functionValues[1].getValue();
    }
  }
}
