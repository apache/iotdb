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

// call function with side effect
public class MethodCallStatement implements Statement {

  private final String variableName;

  private final String methodName;

  private final String[] args;

  public MethodCallStatement(String variableName, String functionName, String... args) {
    this.variableName = variableName;
    this.methodName = functionName;
    this.args = args;
  }

  @Override
  public String toCode() {
    StringBuilder code = new StringBuilder();
    if (variableName != null) {
      code.append(variableName).append(".");
    }

    code.append(methodName).append("(");
    for (String arg : args) {
      code.append(arg).append(", ");
    }

    if (args.length != 0) {
      code.delete(code.length() - 2, code.length());
    }
    return code.append(");\n").toString();
  }
}
