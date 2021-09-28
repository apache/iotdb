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
import org.apache.iotdb.session.Session;

import java.util.List;

public abstract class Selector extends Function {

  // The timestamp corresponding to the value
  private Long timestamp;

  private List<Object> relatedValues;

  public Selector() {}

  public Selector(List<Expression> expressionList) {
    super(expressionList);
  }

  public Selector(List<Expression> expressionList, Session session, String path) {
    super(expressionList, session, path);
  }

  public abstract void updateValueAndRelate(FunctionValue functionValue, List<Object> values);

  public List<Object> getRelatedValues() {
    return this.relatedValues;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public void setRelatedValues(List<Object> relatedValues) {
    this.relatedValues = relatedValues;
  }
}
