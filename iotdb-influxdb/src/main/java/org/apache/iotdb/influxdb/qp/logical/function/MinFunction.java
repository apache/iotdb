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

package org.apache.iotdb.influxdb.qp.logical.function;

import org.apache.iotdb.influxdb.query.expression.Expression;

import java.util.List;

public class MinFunction extends Selector {
  private Double doubleValue = Double.MAX_VALUE;
  private String stringValue = null;
  private boolean isNumber = false;
  private boolean isString = false;

  public MinFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (value instanceof Number) {
      if (!isNumber) {
        isNumber = true;
      }
      double tmpValue = ((Number) value).doubleValue();
      if (tmpValue <= this.doubleValue) {
        doubleValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(values);
      }
    } else if (value instanceof String) {
      String tmpValue = (String) value;
      if (!isString) {
        isString = true;
        stringValue = tmpValue;
        this.setTimestamp(timestamp);
        this.setRelatedValues(values);
      } else {
        if (tmpValue.compareTo(this.stringValue) <= 0) {
          stringValue = tmpValue;
          this.setTimestamp(timestamp);
          this.setRelatedValues(values);
        }
      }
    }
  }

  public MinFunction() {}

  @Override
  public FunctionValue calculate() {
    if (!isString && !isNumber) {
      return new FunctionValue(null, null);
    } else if (isString) {
      return new FunctionValue(stringValue, this.getTimestamp());
    } else {
      return new FunctionValue(doubleValue, this.getTimestamp());
    }
  }
}
