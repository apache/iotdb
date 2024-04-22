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

package org.apache.iotdb.db.queryengine.plan.relational.function;

import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;

public class Signature {

  private final List<TypeVariableConstraint> typeVariableConstraints;
  private final List<LongVariableConstraint> longVariableConstraints;
  private final TypeSignature returnType;
  private final List<TypeSignature> argumentTypes;
  private final boolean variableArity;

  private Signature(
      List<TypeVariableConstraint> typeVariableConstraints,
      List<LongVariableConstraint> longVariableConstraints,
      TypeSignature returnType,
      List<TypeSignature> argumentTypes,
      boolean variableArity) {
    requireNonNull(typeVariableConstraints, "typeVariableConstraints is null");
    requireNonNull(longVariableConstraints, "longVariableConstraints is null");

    this.typeVariableConstraints = new ArrayList<>(typeVariableConstraints);
    this.longVariableConstraints = new ArrayList<>(longVariableConstraints);
    this.returnType = requireNonNull(returnType, "returnType is null");
    this.argumentTypes = new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null"));
    this.variableArity = variableArity;
  }

  public TypeSignature getReturnType() {
    return returnType;
  }

  public List<TypeSignature> getArgumentTypes() {
    return argumentTypes;
  }

  public boolean isVariableArity() {
    return variableArity;
  }

  public List<TypeVariableConstraint> getTypeVariableConstraints() {
    return typeVariableConstraints;
  }

  public List<LongVariableConstraint> getLongVariableConstraints() {
    return longVariableConstraints;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Signature)) {
      return false;
    }
    Signature other = (Signature) obj;
    return Objects.equals(this.typeVariableConstraints, other.typeVariableConstraints)
        && Objects.equals(this.longVariableConstraints, other.longVariableConstraints)
        && Objects.equals(this.returnType, other.returnType)
        && Objects.equals(this.argumentTypes, other.argumentTypes)
        && Objects.equals(this.variableArity, other.variableArity);
  }

  @Override
  public String toString() {
    List<String> allConstraints =
        concat(
                typeVariableConstraints.stream().map(TypeVariableConstraint::toString),
                longVariableConstraints.stream().map(LongVariableConstraint::toString))
            .collect(Collectors.toList());

    return (allConstraints.isEmpty() ? "" : allConstraints.stream().collect(joining(",", "<", ">")))
        + argumentTypes.stream().map(Objects::toString).collect(joining(",", "(", ")"))
        + ":"
        + returnType;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final List<TypeVariableConstraint> typeVariableConstraints = new ArrayList<>();
    private final List<LongVariableConstraint> longVariableConstraints = new ArrayList<>();
    private TypeSignature returnType;
    private final List<TypeSignature> argumentTypes = new ArrayList<>();
    private boolean variableArity;

    private Builder() {}

    public Builder typeVariable(String name) {
      typeVariableConstraints.add(TypeVariableConstraint.builder(name).build());
      return this;
    }

    public Builder comparableTypeParameter(String name) {
      typeVariableConstraints.add(
          TypeVariableConstraint.builder(name).comparableRequired().build());
      return this;
    }

    public Builder orderableTypeParameter(String name) {
      typeVariableConstraints.add(TypeVariableConstraint.builder(name).orderableRequired().build());
      return this;
    }

    public Builder castableToTypeParameter(String name, TypeSignature toType) {
      typeVariableConstraints.add(TypeVariableConstraint.builder(name).castableTo(toType).build());
      return this;
    }

    public Builder castableFromTypeParameter(String name, TypeSignature fromType) {
      typeVariableConstraints.add(
          TypeVariableConstraint.builder(name).castableFrom(fromType).build());
      return this;
    }

    public Builder variadicTypeParameter(String name, String variadicBound) {
      typeVariableConstraints.add(
          TypeVariableConstraint.builder(name).variadicBound(variadicBound).build());
      return this;
    }

    public Builder typeVariableConstraint(TypeVariableConstraint typeVariableConstraint) {
      this.typeVariableConstraints.add(
          requireNonNull(typeVariableConstraint, "typeVariableConstraint is null"));
      return this;
    }

    public Builder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints) {
      this.typeVariableConstraints.addAll(
          requireNonNull(typeVariableConstraints, "typeVariableConstraints is null"));
      return this;
    }

    //    public Builder returnType(Type returnType) {
    //      return returnType(returnType.getTypeSignature());
    //    }

    public Builder returnType(TypeSignature returnType) {
      this.returnType = requireNonNull(returnType, "returnType is null");
      return this;
    }

    public Builder longVariable(String name, String expression) {
      this.longVariableConstraints.add(new LongVariableConstraint(name, expression));
      return this;
    }

    //    public Builder argumentType(Type type) {
    //      return argumentType(type.getTypeSignature());
    //    }

    public Builder argumentType(TypeSignature type) {
      argumentTypes.add(requireNonNull(type, "type is null"));
      return this;
    }

    public Builder argumentTypes(List<TypeSignature> argumentTypes) {
      this.argumentTypes.addAll(requireNonNull(argumentTypes, "argumentTypes is null"));
      return this;
    }

    public Builder variableArity() {
      this.variableArity = true;
      return this;
    }

    public Signature build() {
      return new Signature(
          typeVariableConstraints,
          longVariableConstraints,
          returnType,
          argumentTypes,
          variableArity);
    }
  }
}
