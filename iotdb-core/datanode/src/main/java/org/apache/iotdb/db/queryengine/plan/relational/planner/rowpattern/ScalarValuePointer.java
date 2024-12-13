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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ScalarValuePointer implements ValuePointer {
  private final LogicalIndexPointer logicalIndexPointer;
  private final Symbol inputSymbol;

  public ScalarValuePointer(LogicalIndexPointer logicalIndexPointer, Symbol inputSymbol) {
    this.logicalIndexPointer = requireNonNull(logicalIndexPointer, "logicalIndexPointer is null");
    this.inputSymbol = requireNonNull(inputSymbol, "inputSymbol is null");
  }

  public LogicalIndexPointer getLogicalIndexPointer() {
    return logicalIndexPointer;
  }

  public Symbol getInputSymbol() {
    return inputSymbol;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ScalarValuePointer o = (ScalarValuePointer) obj;
    return Objects.equals(logicalIndexPointer, o.logicalIndexPointer)
        && Objects.equals(inputSymbol, o.inputSymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalIndexPointer, inputSymbol);
  }

  public static void serialize(ScalarValuePointer pointer, ByteBuffer byteBuffer) {
    LogicalIndexPointer.serialize(pointer.logicalIndexPointer, byteBuffer);
    Symbol.serialize(pointer.inputSymbol, byteBuffer);
  }

  public static void serialize(ScalarValuePointer pointer, DataOutputStream stream)
      throws IOException {
    LogicalIndexPointer.serialize(pointer.logicalIndexPointer, stream);
    Symbol.serialize(pointer.inputSymbol, stream);
  }

  public static ScalarValuePointer deserialize(ByteBuffer byteBuffer) {
    LogicalIndexPointer logicalIndexPointer = LogicalIndexPointer.deserialize(byteBuffer);
    Symbol inputSymbol = Symbol.deserialize(byteBuffer);
    return new ScalarValuePointer(logicalIndexPointer, inputSymbol);
  }
}
