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
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;

import java.util.List;

public abstract class InfluxSelector extends InfluxFunction {

  // The timestamp corresponding to the value
  private Long timestamp = null;

  private List<Object> relatedValues;

  protected InfluxSelector(List<Expression> expressionList) {
    super(expressionList);
  }

  public List<Object> getRelatedValues() {
    return this.relatedValues;
  }

  public void setRelatedValues(List<Object> relatedValues) {
    this.relatedValues = relatedValues;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public abstract void updateValueAndRelateValues(
      InfluxFunctionValue functionValue, List<Object> relatedValues);
}
