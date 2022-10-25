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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

import java.util.ArrayList;
import java.util.List;

public class IfStatement implements Statement {

  private ExpressionNode condition;

  private final List<Statement> ifBody;

  private boolean haveElse;

  private List<Statement> elseBody;

  public IfStatement(boolean haveElse) {
    this.haveElse = haveElse;
    ifBody = new ArrayList<>();
    if (haveElse) {
      elseBody = new ArrayList<>();
    }
  }

  public boolean isHaveElse() {
    return haveElse;
  }

  public IfStatement addIfBodyStatement(Statement statement) {
    ifBody.add(statement);
    return this;
  }

  public IfStatement addElseBodyStatement(Statement statement) {
    if (!isHaveElse()) {
      // TODO: throw exception
      return this;
    }
    elseBody.add(statement);
    return this;
  }

  public void setCondition(ExpressionNode condition) {
    this.condition = condition;
  }

  @Override
  public String toCode() {
    StringBuilder ifCode = new StringBuilder();
    ifCode.append("if(").append(condition.toCode()).append("){\n");
    for (Statement s : ifBody) {
      ifCode.append("    ").append(s.toCode());
    }

    if (isHaveElse()) {
      ifCode.append("} else {\n");
      for (Statement s : elseBody) {
        ifCode.append("    ").append(s.toCode());
      }
    }

    ifCode.append("}\n");
    return ifCode.toString();
  }
}
