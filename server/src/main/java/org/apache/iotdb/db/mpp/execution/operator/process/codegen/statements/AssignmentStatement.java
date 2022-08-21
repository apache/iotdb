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

public class AssignmentStatement implements Statement {
  private final String varName;

  private final ExpressionNode es;

  private final CodegenDataType type;

  public AssignmentStatement(String varName, ExpressionNode es, CodegenDataType type) {
    this.varName = varName;
    this.es = es;
    this.type = type;
  }

  @Override
  public String toCode() {
    return varName + " = " + type + ".valueOf(" + es.toCode() + ");\n";
  }
}
