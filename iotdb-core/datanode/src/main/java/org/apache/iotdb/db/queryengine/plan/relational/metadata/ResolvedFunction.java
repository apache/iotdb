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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ResolvedFunction {
  private final BoundSignature signature;
  private final FunctionId functionId;
  private final FunctionKind functionKind;
  private final boolean deterministic;

  private final FunctionNullability functionNullability;

  public ResolvedFunction(
      BoundSignature signature,
      FunctionId functionId,
      FunctionKind functionKind,
      boolean deterministic,
      FunctionNullability functionNullability) {
    this.signature = requireNonNull(signature, "signature is null");
    this.functionId = requireNonNull(functionId, "functionId is null");
    this.functionKind = requireNonNull(functionKind, "functionKind is null");
    this.deterministic = deterministic;
    this.functionNullability = requireNonNull(functionNullability, "functionNullability is null");
    ;
  }

  public BoundSignature getSignature() {
    return signature;
  }

  public FunctionId getFunctionId() {
    return functionId;
  }

  public FunctionKind getFunctionKind() {
    return functionKind;
  }

  public boolean isDeterministic() {
    return deterministic;
  }

  public FunctionNullability getFunctionNullability() {
    return functionNullability;
  }

  //  public static boolean isResolved(QualifiedName name) {
  //    return SerializedResolvedFunction.isSerializedResolvedFunction(name);
  //  }
  //
  public QualifiedName toQualifiedName() {
    return QualifiedName.of(signature.getName());
  }

  //
  //  public CatalogSchemaFunctionName toCatalogSchemaFunctionName() {
  //    return ResolvedFunctionDecoder.toCatalogSchemaFunctionName(this);
  //  }
  //
  //  public static CatalogSchemaFunctionName extractFunctionName(QualifiedName qualifiedName) {
  //    checkArgument(isResolved(qualifiedName), "Expected qualifiedName to be a resolved function:
  // %s", qualifiedName);
  //    return SerializedResolvedFunction.fromSerializedName(qualifiedName).functionName();
  //  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResolvedFunction that = (ResolvedFunction) o;
    return Objects.equals(signature, that.signature)
        && Objects.equals(functionId, that.functionId)
        && functionKind == that.functionKind
        && deterministic == that.deterministic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(signature, functionId, functionKind, deterministic);
  }

  @Override
  public String toString() {
    return signature.toString();
  }
}
