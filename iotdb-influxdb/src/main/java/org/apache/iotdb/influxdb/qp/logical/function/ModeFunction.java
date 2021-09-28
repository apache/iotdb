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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModeFunction extends Aggregate {
  private Map<Object, Integer> valueOrders = new HashMap<>();
  private Map<Object, Long> valueLastTimestamp = new HashMap<>();
  private int maxNumber = 0;
  private Object maxObject = null;

  public ModeFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  public ModeFunction() {}

  @Override
  public void updateValue(FunctionValue functionValue) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    // update new data
    if (!valueOrders.containsKey(value)) {
      valueOrders.put(value, 1);
      valueLastTimestamp.put(value, timestamp);
    } else {
      valueOrders.put(value, valueOrders.get(value) + 1);
      if (timestamp < valueLastTimestamp.get(value)) {
        valueLastTimestamp.put(value, timestamp);
      }
    }
    // Judge whether the new data meets the conditions
    if (maxObject == null) {
      maxObject = value;
      maxNumber = 1;
    } else {
      if (valueOrders.get(value) > maxNumber) {
        maxNumber = valueOrders.get(value);
        maxObject = value;
      } else if (valueOrders.get(value) == maxNumber
          && timestamp < valueLastTimestamp.get(maxObject)) {
        maxObject = value;
      }
    }
  }

  @Override
  public FunctionValue calculate() {
    return new FunctionValue(maxObject, 0L);
  }

  @Override
  public FunctionValue calculateByIoTDBFunc() {
    return null;
  }
}
