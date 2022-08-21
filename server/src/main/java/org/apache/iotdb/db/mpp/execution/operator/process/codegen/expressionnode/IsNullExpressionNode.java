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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode;

public class IsNullExpressionNode extends ExpressionNodeImpl {
  private ExpressionNode subExpression;

  private boolean isNotNull;

  public IsNullExpressionNode(String nodeName, ExpressionNode subExpression, boolean isNotNull) {
    this.isNotNull = isNotNull;
    this.nodeName = nodeName;
    this.subExpression = subExpression;
  }

  // subExpressionNode of IsNullExpressionNode should always have a name
  public IsNullExpressionNode(ExpressionNode subExpression, boolean isNotNull) {
    this.nodeName = null;
    this.subExpression = subExpression;
    this.isNotNull = isNotNull;
  }

  @Override
  public String toCode() {
    String op = isNotNull ? "!=" : "==";
    if (subExpression.getNodeName() != null) {
      return subExpression.getNodeName() + " " + op + " null ";
    }
    // this should not happen, and may be meaningless
    return bracket(subExpression.toCode()) + " " + op + " null";
  }

  @Override
  public ExpressionNode checkWhetherNotNull() {
    return new ConstantExpressionNode("true");
  }

  @Override
  public String toSingleRowCode() {
    String op = isNotNull ? "!=" : "==";
    return subExpression.toSingleRowCode() + " " + op + " null ";
  }
}
