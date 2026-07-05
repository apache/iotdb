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

package org.apache.iotdb.commons.queryengine.plan.relational.function;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResolvedFunctionCallBuilder {
  private final ResolvedFunction resolvedFunction;
  private List<Expression> argumentValues = new ArrayList<>();

  public static ResolvedFunctionCallBuilder builder(ResolvedFunction resolvedFunction) {
    return new ResolvedFunctionCallBuilder(resolvedFunction);
  }

  private ResolvedFunctionCallBuilder(ResolvedFunction resolvedFunction) {
    this.resolvedFunction =
        requireNonNull(resolvedFunction, QueryMessages.EXCEPTION_RESOLVEDFUNCTION_IS_NULL_81B5B93A);
  }

  public ResolvedFunctionCallBuilder addArgument(Expression value) {
    requireNonNull(value, QueryMessages.EXCEPTION_VALUE_IS_NULL_192F6BFF);
    argumentValues.add(value);
    return this;
  }

  public ResolvedFunctionCallBuilder setArguments(List<Expression> values) {
    requireNonNull(values, QueryMessages.EXCEPTION_VALUES_IS_NULL_F1D7D3D8);
    argumentValues = new ArrayList<>(values);
    return this;
  }

  public FunctionCall build() {
    return new FunctionCall(resolvedFunction.toQualifiedName(), argumentValues);
  }
}
