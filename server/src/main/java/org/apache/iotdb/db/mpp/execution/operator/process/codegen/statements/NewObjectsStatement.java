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
import java.util.Objects;

public class NewObjectsStatement extends DeclareStatement {
  private final List<ExpressionNode> retValues;

  private final int size;

  public NewObjectsStatement(String varName, int size) {
    this.varName = varName;
    this.size = size;
    this.retValues = new ArrayList<>();
  }

  public void addRetValue(ExpressionNode expressionNode) {
    retValues.add(expressionNode);
  }

  @Override
  public String toCode() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("Object[] ")
        .append(varName)
        .append(" = ")
        .append("new Object[ " + size + " ];\n");
    for (int i = 0; i < retValues.size(); ++i) {
      if (!Objects.isNull(retValues.get(i))) {
        stringBuilder
            .append(varName)
            .append("[ " + i + " ] = ")
            .append(retValues.get(i).getNodeName())
            .append(";\n");
      }
    }
    return stringBuilder.toString();
  }
}
