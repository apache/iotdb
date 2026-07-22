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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class AggregationValuePointer implements ValuePointer {
  private final ResolvedFunction function;
  private final AggregationLabelSet setDescriptor;
  private final List<Expression> arguments;
  private final Optional<Symbol> classifierSymbol;
  private final Optional<Symbol> matchNumberSymbol;

  public AggregationValuePointer(
      ResolvedFunction function,
      AggregationLabelSet setDescriptor,
      List<Expression> arguments,
      Optional<Symbol> classifierSymbol,
      Optional<Symbol> matchNumberSymbol) {
    this.function = requireNonNull(function, QueryMessages.EXCEPTION_FUNCTION_IS_NULL_E0FA4B62);
    this.setDescriptor =
        requireNonNull(setDescriptor, QueryMessages.EXCEPTION_SETDESCRIPTOR_IS_NULL_4ED0D19A);
    this.arguments = requireNonNull(arguments, QueryMessages.EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2);
    this.classifierSymbol =
        requireNonNull(classifierSymbol, QueryMessages.EXCEPTION_CLASSIFIERSYMBOL_IS_NULL_B92EE093);
    this.matchNumberSymbol =
        requireNonNull(
            matchNumberSymbol, QueryMessages.EXCEPTION_MATCHNUMBERSYMBOL_IS_NULL_D88DC4EE);
  }

  public ResolvedFunction getFunction() {
    return function;
  }

  public AggregationLabelSet getSetDescriptor() {
    return setDescriptor;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  public Optional<Symbol> getClassifierSymbol() {
    return classifierSymbol;
  }

  public Optional<Symbol> getMatchNumberSymbol() {
    return matchNumberSymbol;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    AggregationValuePointer o = (AggregationValuePointer) obj;
    return Objects.equals(function, o.function)
        && Objects.equals(setDescriptor, o.setDescriptor)
        && Objects.equals(arguments, o.arguments)
        && Objects.equals(classifierSymbol, o.classifierSymbol)
        && Objects.equals(matchNumberSymbol, o.matchNumberSymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, setDescriptor, arguments, classifierSymbol, matchNumberSymbol);
  }

  public static void serialize(AggregationValuePointer pointer, ByteBuffer byteBuffer) {
    pointer.function.serialize(byteBuffer);
    AggregationLabelSet.serialize(pointer.setDescriptor, byteBuffer);
    byteBuffer.putInt(pointer.arguments.size());
    for (Expression arg : pointer.arguments) {
      Expression.serialize(arg, byteBuffer);
    }
    byteBuffer.put(pointer.classifierSymbol.isPresent() ? (byte) 1 : (byte) 0);
    if (pointer.classifierSymbol.isPresent()) {
      Symbol.serialize(pointer.classifierSymbol.get(), byteBuffer);
    }
    byteBuffer.put(pointer.matchNumberSymbol.isPresent() ? (byte) 1 : (byte) 0);
    if (pointer.matchNumberSymbol.isPresent()) {
      Symbol.serialize(pointer.matchNumberSymbol.get(), byteBuffer);
    }
  }

  public static void serialize(AggregationValuePointer pointer, DataOutputStream stream)
      throws IOException {
    pointer.function.serialize(stream);
    AggregationLabelSet.serialize(pointer.setDescriptor, stream);
    stream.writeInt(pointer.arguments.size());
    for (Expression arg : pointer.arguments) {
      Expression.serialize(arg, stream);
    }
    stream.writeBoolean(pointer.classifierSymbol.isPresent());
    if (pointer.classifierSymbol.isPresent()) {
      Symbol.serialize(pointer.classifierSymbol.get(), stream);
    }
    stream.writeBoolean(pointer.matchNumberSymbol.isPresent());
    if (pointer.matchNumberSymbol.isPresent()) {
      Symbol.serialize(pointer.matchNumberSymbol.get(), stream);
    }
  }

  public static AggregationValuePointer deserialize(ByteBuffer byteBuffer) {
    ResolvedFunction function = ResolvedFunction.deserialize(byteBuffer);
    AggregationLabelSet setDescriptor = AggregationLabelSet.deserialize(byteBuffer);
    int argCount = byteBuffer.getInt();
    List<Expression> arguments = new ArrayList<>(argCount);
    for (int i = 0; i < argCount; i++) {
      arguments.add(Expression.deserialize(byteBuffer));
    }
    Optional<Symbol> classifierSymbol =
        byteBuffer.get() == 1 ? Optional.of(Symbol.deserialize(byteBuffer)) : Optional.empty();
    Optional<Symbol> matchNumberSymbol =
        byteBuffer.get() == 1 ? Optional.of(Symbol.deserialize(byteBuffer)) : Optional.empty();
    return new AggregationValuePointer(
        function, setDescriptor, arguments, classifierSymbol, matchNumberSymbol);
  }
}
