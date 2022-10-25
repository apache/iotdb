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

/**
 * This class is used to help create UDTF input row. It just update a row but never create a new one
 */
public class UpdateRowStatement implements Statement {
  private List<ExpressionNode> columns;

  private final String varName;

  public UpdateRowStatement(String varName) {
    this.varName = varName;
  }

  public void addData(ExpressionNode ExpressionNode) {
    if (Objects.isNull(columns)) {
      columns = new ArrayList<>();
    }
    columns.add(ExpressionNode);
  }

  public void setColumns(List<ExpressionNode> columns) {
    this.columns = columns;
  }

  @Override
  public String toCode() {
    StringBuilder newRow = new StringBuilder();
    newRow.append(varName).append(".setData(timestamp");
    for (ExpressionNode ExpressionNode : columns) {
      newRow.append(", ").append(ExpressionNode.getNodeName());
    }
    newRow.append(");\n");
    return newRow.toString();
  }
}
