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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
    this.function = requireNonNull(function, "function is null");
    this.setDescriptor = requireNonNull(setDescriptor, "setDescriptor is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
    this.classifierSymbol = requireNonNull(classifierSymbol, "classifierSymbol is null");
    this.matchNumberSymbol = requireNonNull(matchNumberSymbol, "matchNumberSymbol is null");
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

  public List<Symbol> getInputSymbols() {
    return arguments.stream()
        .map(SymbolsExtractor::extractAll)
        .flatMap(Collection::stream)
        .filter(
            symbol ->
                (!classifierSymbol.isPresent() || !classifierSymbol.get().equals(symbol))
                    && (!matchNumberSymbol.isPresent() || !matchNumberSymbol.get().equals(symbol)))
        .collect(toImmutableList());
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
