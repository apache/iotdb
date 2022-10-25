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

import java.util.ArrayList;
import java.util.List;

public class BinaryExpressionNode extends ExpressionNodeImpl {

  private final String op;

  private final ExpressionNode leftNode;

  private final ExpressionNode rightNode;

  public BinaryExpressionNode(
      String nodeName, String op, ExpressionNode leftNode, ExpressionNode rightNode) {
    this.nodeName = nodeName;
    this.op = op;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  public BinaryExpressionNode(String op, ExpressionNode leftNode, ExpressionNode rightNode) {
    this.nodeName = null;
    this.op = op;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  @Override
  public String toCode() {
    StringBuilder binaryExpressionCode = new StringBuilder();
    if (leftNode.getNodeName() != null) {
      binaryExpressionCode.append(leftNode.getNodeName());
    } else {
      binaryExpressionCode.append(bracket(leftNode.toCode()));
    }
    binaryExpressionCode.append(op);
    if (rightNode.getNodeName() != null) {
      binaryExpressionCode.append(rightNode.getNodeName());
    } else {
      binaryExpressionCode.append(bracket(rightNode.toCode()));
    }
    return binaryExpressionCode.toString();
  }

  @Override
  public List<String> getIsNullCheckNodes() {
    ArrayList<String> subNodes = new ArrayList<>();
    subNodes.add(leftNode.getNodeName());
    subNodes.add(rightNode.getNodeName());
    return subNodes;
  }
}
