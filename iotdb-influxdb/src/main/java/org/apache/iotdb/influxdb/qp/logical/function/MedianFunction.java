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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianFunction extends Aggregate {
  private List<Double> numbers = new ArrayList<>();

  public MedianFunction(List<Expression> expressionList) {
    super(expressionList);
  }

  @Override
  public void updateValue(FunctionValue functionValue) {
    Object value = functionValue.getValue();
    if (value instanceof Number) {
      numbers.add(((Number) value).doubleValue());
    } else {
      throw new IllegalArgumentException("not support this type");
    }
  }

  public MedianFunction() {}

  @Override
  public FunctionValue calculate() {
    Collections.sort(numbers);
    int len = numbers.size();
    if (len % 2 == 0) {
      return new FunctionValue((numbers.get(len / 2) + numbers.get(len / 2 - 1)) / 2, 0L);

    } else {
      return new FunctionValue(numbers.get(len / 2), 0L);
    }
  }

  @Override
  public FunctionValue calculateByIotdbFunc() {
    return null;
  }
}
