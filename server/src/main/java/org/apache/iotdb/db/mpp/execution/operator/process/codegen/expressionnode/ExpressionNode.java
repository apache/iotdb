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

import java.util.List;

/**
 * This type is a expression node help to generate expression string it is not complete java
 * statement
 */
public interface ExpressionNode {

  /**
   * get the code string of the expression if subNode has name, it will replace the full code of the
   * subNode with its nodeName for example: a + b * c if subNode a * c has name "var1", the return
   * value will be a + var1
   */
  String toCode();

  /** @return name of this intermediate variable */
  String getNodeName();

  /** @return all intermediate variables which can cause this variable to be null */
  List<String> getIsNullCheckNodes();

  void setNodeName(String nodeName);
}
