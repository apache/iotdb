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

public class WhileStatement implements Statement {
  private final ExpressionNode condition;

  private List<Statement> loopBody;

  public WhileStatement(ExpressionNode condition) {
    this.condition = condition;
    loopBody = new ArrayList<>();
  }

  public void addLoopStatement(Statement statement) {
    loopBody.add(statement);
  }

  @Override
  public String toCode() {
    StringBuilder whileStatementCode = new StringBuilder();

    whileStatementCode.append("while(").append(condition.toCode()).append("){\n");

    for (Statement s : loopBody) {
      whileStatementCode.append("  ").append(s.toCode()).append("\n");
    }

    whileStatementCode.append("}");
    return whileStatementCode.toString();
  }
}
