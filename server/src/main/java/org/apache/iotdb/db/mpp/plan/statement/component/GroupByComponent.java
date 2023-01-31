/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.HashMap;
import java.util.Map;

public class GroupByComponent extends StatementNode {

  private Expression ControlColumnExpression;
  private double delta = 0;

  Map<String, String> pair = null;

  private final WindowType windowType;

  public GroupByComponent(WindowType windowType) {
    this.windowType = windowType;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public double getDelta() {
    return delta;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }

  public void setControlColumnExpression(Expression controlColumnExpression) {
    ControlColumnExpression = controlColumnExpression;
  }

  public Expression getControlColumnExpression() {
    return ControlColumnExpression;
  }

  public void setPair(String key, String value) {
    if (pair == null) {
      pair = new HashMap<>();
    }
    pair.put(key, value);
  }

  public Map<String, String> getPair() {
    return pair;
  }
}
