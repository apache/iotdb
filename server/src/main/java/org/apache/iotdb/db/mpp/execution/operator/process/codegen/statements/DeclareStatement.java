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
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares.CodegenDataType;

public abstract class DeclareStatement implements Statement {

  // not only declare a new variable
  // this class will also check whether current value is null
  protected String varName;

  protected ExpressionNode es;

  protected CodegenDataType type;

  protected IfStatement ifNull;

  protected void generateNullCheck() {
    ifNull = new IfStatement();
    ifNull.setHaveElse(false);
    ifNull.setCondition(es.checkWhetherNotNull());
    ifNull.addIfBodyStatement(new AssignmentStatement(varName, es, type));
  }

  @Override
  public String toCode() {
    StringBuilder newStatement = new StringBuilder();
    newStatement.append(type).append(" ").append(varName).append(" = null;\n");

    newStatement.append(ifNull.toCode());
    return newStatement.toString();
  }
}
