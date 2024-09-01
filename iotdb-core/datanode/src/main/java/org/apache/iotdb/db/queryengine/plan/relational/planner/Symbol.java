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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Symbol implements Comparable<Symbol> {
  private final String name;

  public static Symbol from(Expression expression) {
    checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
    return new Symbol(((SymbolReference) expression).getName());
  }

  public Symbol(String name) {
    requireNonNull(name, "name is null");
    this.name = name;
  }

  public static Symbol of(String name) {
    requireNonNull(name, "name is null");
    return new Symbol(name);
  }

  public String getName() {
    return name;
  }

  public SymbolReference toSymbolReference() {
    return new SymbolReference(name);
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Symbol symbol = (Symbol) o;

    return name.equals(symbol.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public int compareTo(Symbol o) {
    return name.compareTo(o.name);
  }

  public static void serialize(Symbol symbol, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(symbol.getName(), byteBuffer);
  }

  public static void serialize(Symbol symbol, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(symbol.getName(), stream);
  }

  public static Symbol deserialize(ByteBuffer byteBuffer) {
    return new Symbol(ReadWriteIOUtils.readString(byteBuffer));
  }
}
