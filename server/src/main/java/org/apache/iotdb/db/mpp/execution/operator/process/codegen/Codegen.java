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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public interface Codegen {
  /** Accept inputs in Objects, return calculated value through code generation */
  Object[] accept(Object[] args, long timestamp) throws InvocationTargetException;

  /** construct expression to script, may fail and will return false */
  boolean addExpression(Expression expression);

  List<Boolean> isGenerated();

  Codegen setTypeProvider(TypeProvider typeProvider);

  /** this function should be called before {@link Codegen#addExpression(Expression)} */
  Codegen setInputs(List<String> paths, List<TSDataType> tsDataTypes);

  void generateScriptEvaluator() throws Exception;
}
