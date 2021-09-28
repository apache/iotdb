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
import org.apache.iotdb.influxdb.query.expression.unary.NodeExpression;
import org.apache.iotdb.session.Session;

import java.util.List;

public abstract class Function {

  // contain possible parameters
  private List<Expression> expressionList;

  protected Session session;

  protected String path;

  public Function(List<Expression> expressionList) {
    this.expressionList = expressionList;
  }

  public Function(List<Expression> expressionList, Session session, String path) {
    this.expressionList = expressionList;
    this.session = session;
    this.path = path;
  }

  public Function() {}

  // calculate result
  public abstract FunctionValue calculate();

  public abstract FunctionValue calculateByIoTDBFunc();

  public List<Expression> getExpressions() {
    return this.expressionList;
  }

  public Session getSession() {
    return this.session;
  }

  public String getParmaName() {
    if (expressionList == null) {
      throw new IllegalArgumentException("not support param");
    }
    NodeExpression parmaExpression = (NodeExpression) expressionList.get(0);
    return parmaExpression.getName();
  }
}
