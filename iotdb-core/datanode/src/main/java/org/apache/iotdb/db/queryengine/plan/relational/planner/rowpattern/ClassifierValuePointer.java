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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ClassifierValuePointer implements ValuePointer {
  private final LogicalIndexPointer logicalIndexPointer;

  public ClassifierValuePointer(LogicalIndexPointer logicalIndexPointer) {
    this.logicalIndexPointer = requireNonNull(logicalIndexPointer, "logicalIndexPointer is null");
  }

  public LogicalIndexPointer getLogicalIndexPointer() {
    return logicalIndexPointer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClassifierValuePointer that = (ClassifierValuePointer) o;
    return Objects.equals(logicalIndexPointer, that.logicalIndexPointer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalIndexPointer);
  }

  public static void serialize(ClassifierValuePointer pointer, ByteBuffer byteBuffer) {
    LogicalIndexPointer.serialize(pointer.logicalIndexPointer, byteBuffer);
  }

  public static void serialize(ClassifierValuePointer pointer, DataOutputStream stream)
      throws IOException {
    LogicalIndexPointer.serialize(pointer.logicalIndexPointer, stream);
  }

  public static ClassifierValuePointer deserialize(ByteBuffer byteBuffer) {
    LogicalIndexPointer logicalIndexPointer = LogicalIndexPointer.deserialize(byteBuffer);
    return new ClassifierValuePointer(logicalIndexPointer);
  }
}
