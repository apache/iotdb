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

public class LastFunction extends Selector {
  private Object value;

  public LastFunction(List<Expression> expressionList) {
    super(expressionList);
    this.setTimestamp(Long.MIN_VALUE);
  }

  public LastFunction() {}

  @Override
  public void updateValueAndRelate(FunctionValue functionValue, List<Object> values) {

    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (timestamp >= this.getTimestamp()) {
      this.value = value;
      this.setTimestamp(timestamp);
      this.setRelatedValues(values);
    }
  }

  @Override
  public FunctionValue calculate() {
    return new FunctionValue(value, this.getTimestamp());
  }
}
