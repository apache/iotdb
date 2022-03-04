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

import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionValue;
import org.apache.iotdb.db.query.expression.Expression;

import java.util.List;

public class InfluxDBFirstFunction extends InfluxDBSelector {
  private Object value;

  public InfluxDBFirstFunction(List<Expression> expressionList) {
    super(expressionList);
    this.setTimestamp(Long.MIN_VALUE);
  }

  public InfluxDBFirstFunction(List<Expression> expressionList, String path) {
    super(expressionList, path);
  }

  @Override
  public InfluxDBFunctionValue calculate() {
    return new InfluxDBFunctionValue(value, this.getTimestamp());
  }

  @Override
  public InfluxDBFunctionValue calculateByIoTDBFunc() {
    return null;
  }

  @Override
  public void updateValueAndRelateValues(
      InfluxDBFunctionValue functionValue, List<Object> relatedValues) {
    Object value = functionValue.getValue();
    Long timestamp = functionValue.getTimestamp();
    if (timestamp <= this.getTimestamp()) {
      this.value = value;
      this.setTimestamp(timestamp);
      this.setRelatedValues(relatedValues);
    }
  }
}
