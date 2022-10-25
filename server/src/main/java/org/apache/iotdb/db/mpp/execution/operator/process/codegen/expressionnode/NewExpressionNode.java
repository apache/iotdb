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

public class NewExpressionNode extends ExpressionNodeImpl {

  private String type;

  private String[] args;

  public NewExpressionNode(String type, String... args) {
    this.type = type;
    this.args = args;
  }

  @Override
  public String toCode() {
    StringBuilder code = new StringBuilder();
    code.append("new ").append(type).append("(");
    for (int i = 0; i < args.length; i++) {
      code.append(args[i]).append(", ");
    }
    return code.delete(code.length() - 2, code.length()).append(")").toString();
  }

  @Override
  public List<String> getIsNullCheckNodes() {
    return new ArrayList<>();
  }
}
