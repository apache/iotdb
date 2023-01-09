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

public class InfluxMaxFunction extends InfluxSelector {
  private Double doubleValue = Double.MIN_VALUE;
  private String stringValue = null;
  private boolean isNumber = false;
  private boolean isString = false;

  private Double maxValue = null;

  public InfluxMaxFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public InfluxFunctionValue calculateBruteForce() {
    if (!isString && !isNumber) {
      return new InfluxFunctionValue(null, null);
    } else if (isString) {
      return new InfluxFunctionValue(stringValue, this.getTimestamp());
    } else {
      return new InfluxFunctionValue(doubleValue, this.getTimestamp());
    }
  }

  @Override
  public InfluxFunctionValue calculateByIoTDBFunc() {
    return new InfluxFunctionValue(maxValue, maxValue == null ? null : 0L);
  }

  @Override
  public void updateValueIoTDBFunc(InfluxFunctionValue... functionValues) {
    if (functionValues[0].getValue() instanceof Number) {
      double tmpValue = ((Number) functionValues[0].getValue()).doubleValue();
      if (maxValue == null) {
        maxValue = tmpValue;
      } else if (tmpValue > maxValue) {
        maxValue = tmpValue;
      }
    }
  }

  @Override
  public String getFunctionName() {
    return InfluxSqlConstant.MAX;
  }

  @Override
  public void updateValueAndRelateValues(
      InfluxFunctionValue functionValue, List<Object> relatedValues) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (value instanceof Number) {
      if (!isNumber) {
        isNumber = true;
      }
      double tmpValue = ((Number) value).doubleValue();
      if (tmpValue >= this.doubleValue) {
        doubleValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      }
    } else if (value instanceof String) {
      String tmpValue = (String) value;
      if (!isString) {
        isString = true;
        stringValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      } else if (tmpValue.compareTo(this.stringValue) >= 0) {
        stringValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(relatedValues);
      }
    }
  }
}
