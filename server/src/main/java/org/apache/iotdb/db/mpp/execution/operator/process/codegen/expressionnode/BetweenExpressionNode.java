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

public class BetweenExpressionNode extends ExpressionNodeImpl {
  private final ExpressionNode firstNode;
  private final ExpressionNode secondNode;
  private final ExpressionNode thirdNode;
  private final boolean isNotBetween;

  public BetweenExpressionNode(
      String nodeName,
      ExpressionNode firstNode,
      ExpressionNode secondNode,
      ExpressionNode thirdNode,
      boolean isNotBetween) {
    this.firstNode = firstNode;
    this.secondNode = secondNode;
    this.thirdNode = thirdNode;
    this.nodeName = nodeName;
    this.isNotBetween = isNotBetween;
  }

  @Override
  public String toCode() {
    StringBuilder betweenCode = new StringBuilder();
    betweenCode
        .append(firstNode.getNodeName())
        .append(">=")
        .append(secondNode.getNodeName())
        .append("&&")
        .append(firstNode.getNodeName())
        .append("<=")
        .append(thirdNode.getNodeName());
    return isNotBetween ? "!" + bracket(betweenCode.toString()) : betweenCode.toString();
  }

  @Override
  public List<String> getIsNullCheckNodes() {
    ArrayList<String> subNodes = new ArrayList<>();
    subNodes.add(firstNode.getNodeName());
    subNodes.add(secondNode.getNodeName());
    subNodes.add(thirdNode.getNodeName());
    return subNodes;
  }
}
