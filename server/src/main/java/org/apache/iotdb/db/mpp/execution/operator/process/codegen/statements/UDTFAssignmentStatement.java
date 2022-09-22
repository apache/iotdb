/* *
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

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.CodeExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.FunctionExpressionNode;

import java.util.ArrayList;
import java.util.List;

// This class is used to help generate AssignmentStatement of UDTF
public class UDTFAssignmentStatement extends AssignmentStatement {
  private final List<Statement> ifBodyStatements;

  public UDTFAssignmentStatement(
      FunctionExpressionNode functionExpressionNode, UpdateRowStatement updateRowStatement) {
    super(functionExpressionNode);
    this.ifBodyStatements = new ArrayList<>();

    String objectName = getNodeName() + "Object";
    ifBodyStatements.add(updateRowStatement);
    ifBodyStatements.add(new DeclareStatement("Object", objectName));
    ifBodyStatements.add(new AssignmentStatement(objectName, getExpressionNode()));

    IfStatement objectIsNull = new IfStatement(true);
    objectIsNull.setCondition(new ConstantExpressionNode(objectName + " == null"));
    objectIsNull.addIfBodyStatement(
        new AssignmentStatement(getNodeName() + "IsNull", new ConstantExpressionNode("true")));
    objectIsNull.addElseBodyStatement(
        new AssignmentStatement(
            getNodeName(),
            new CodeExpressionNode("(" + functionExpressionNode.getType() + ")" + objectName)));
    ifBodyStatements.add(objectIsNull);
  }

  @Override
  public String toCode() {
    StringBuilder code = new StringBuilder();
    for (Statement statement : ifBodyStatements) {
      code.append(statement.toCode());
    }
    return code.toString();
  }

  @Override
  public ExpressionNode getNullCondition() {
    List<String> subNodes = getExpressionNode().getIsNullCheckNodes();
    if (subNodes == null || subNodes.size() == 0) {
      // no condition, always treat as variable will not be null
      return new ConstantExpressionNode("false");
    }
    StringBuilder conditionCode = new StringBuilder();
    for (String subNodeName : subNodes) {
      if (subNodeName != null) {
        conditionCode.append(subNodeName).append("IsNull && ");
      }
    }

    conditionCode.delete(conditionCode.length() - 4, conditionCode.length());
    return new ConstantExpressionNode(conditionCode.toString());
  }
}
