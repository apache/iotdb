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

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;

import java.util.List;

// assign value to a variable
public class AssignmentStatement implements Statement {
  private final String varName;

  private final ExpressionNode es;

  public AssignmentStatement(String varName, ExpressionNode es) {
    this.varName = varName;
    this.es = es;
  }

  public AssignmentStatement(ExpressionNode es) {
    this.es = es;
    this.varName = es.getNodeName();
  }

  public String getVarName() {
    return varName;
  }

  public ExpressionNode getExpressionNode() {
    return es;
  }

  public String getNodeName() {
    return es.getNodeName();
  }

  @Override
  public String toCode() {
    return varName + " = " + es.toCode() + ";\n";
  }

  public ExpressionNode getNullCondition() {
    List<String> subNodes = es.getIsNullCheckNodes();
    if (subNodes == null || subNodes.size() == 0) {
      // no condition, always treat as variable will not be null
      return new ConstantExpressionNode("false");
    }
    StringBuilder conditionCode = new StringBuilder();
    for (String subNodeName : subNodes) {
      if (subNodeName != null) {
        conditionCode.append(subNodeName).append("IsNull || ");
      }
    }

    conditionCode.delete(conditionCode.length() - 4, conditionCode.length());
    return new ConstantExpressionNode(conditionCode.toString());
  }
}
