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

public class IsNullExpressionNode extends ExpressionNodeImpl {
  private final ExpressionNode subExpression;

  private final boolean isNotNull;

  public IsNullExpressionNode(String nodeName, ExpressionNode subExpression, boolean isNotNull) {
    this.isNotNull = isNotNull;
    this.nodeName = nodeName;
    this.subExpression = subExpression;
  }

  @Override
  public String toCode() {
    String bool = isNotNull ? "false" : "true";
    return subExpression.getNodeName() + "IsNull == " + bool;
  }

  @Override
  public List<String> getIsNullCheckNodes() {
    return new ArrayList<>();
  }
}
