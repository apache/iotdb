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

public class InExpressionNode extends ExpressionNodeImpl {
  private final String setName;

  private final ExpressionNode subNode;

  private final boolean isNotIn;

  public InExpressionNode(
      String nodeName, ExpressionNode subNode, String setNameNode, boolean isNotIn) {
    this.nodeName = nodeName;
    this.setName = setNameNode;
    this.subNode = subNode;
    this.isNotIn = isNotIn;
  }

  public InExpressionNode(ExpressionNode subNode, String setName, boolean isNotIn) {
    this.setName = setName;
    this.subNode = subNode;
    this.isNotIn = isNotIn;
  }

  @Override
  public String toCode() {
    if (subNode.getNodeName() != null) {
      String code = setName + ".contains(" + subNode.getNodeName() + ")";
      return isNotIn ? "! " + code : code;
    } else {
      String code = setName + ".contains(" + subNode.toCode() + ")";
      return isNotIn ? "! " + code : code;
    }
  }

  @Override
  public List<String> getIsNullCheckNodes() {
    ArrayList<String> subNodes = new ArrayList<>();
    subNodes.add(subNode.getNodeName());
    return subNodes;
  }
}
