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

import java.util.List;

public class InfluxSpreadFunction extends InfluxAggregator {
  private Double maxNum = null;
  private Double minNum = null;

  public InfluxSpreadFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    if (maxNum == null || minNum == null) {
      return new InfluxFunctionValue(null, 0L);
    } else {
      return new InfluxFunctionValue(maxNum - minNum, 0L);
    }
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    if (maxNum == null || minNum == null) {
      return new InfluxFunctionValue(null, null);
    }
    return new InfluxFunctionValue(maxNum - minNum, 0L);
  }

  @Override
  public String getFunctionName() {
    return InfluxSqlConstant.SPREAD;
  }

  @Override
  public void updateValueBruteForce(InfluxFunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (!(value instanceof Number)) {
      throw new IllegalArgumentException("not support this type");
    }

    double tmpValue = ((Number) value).doubleValue();
    if (maxNum == null || tmpValue > maxNum) {
      maxNum = tmpValue;
    }
    if (minNum == null || tmpValue < minNum) {
      minNum = tmpValue;
    }
  }

  @Override
  public void updateValueIoTDBFunc(InfluxFunctionValue... functionValues) {
    if (functionValues.length == 1) {
      double tmpValue = ((Number) functionValues[0].getValue()).doubleValue();
      if (maxNum == null) {
        maxNum = tmpValue;
      } else if (tmpValue > maxNum) {
        maxNum = tmpValue;
      }
    } else if (functionValues.length == 2) {
      double tmpValue = ((Number) functionValues[1].getValue()).doubleValue();
      if (minNum == null) {
        minNum = tmpValue;
      } else if (tmpValue < minNum) {
        minNum = tmpValue;
      }
    }
  }
}
