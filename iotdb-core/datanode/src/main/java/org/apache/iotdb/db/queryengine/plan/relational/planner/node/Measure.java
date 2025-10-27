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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil;

import org.apache.tsfile.read.common.type.Type;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Measure {
  private final ExpressionAndValuePointers expressionAndValuePointers;
  private final Type type;

  public Measure(ExpressionAndValuePointers expressionAndValuePointers, Type type) {
    this.expressionAndValuePointers =
        requireNonNull(expressionAndValuePointers, "expressionAndValuePointers is null");
    this.type = requireNonNull(type, "type is null");
  }

  public ExpressionAndValuePointers getExpressionAndValuePointers() {
    return expressionAndValuePointers;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Measure that = (Measure) o;
    return Objects.equals(expressionAndValuePointers, that.expressionAndValuePointers)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressionAndValuePointers, type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("expressionAndValuePointers", expressionAndValuePointers)
        .add("type", type)
        .toString();
  }

  public static void serialize(Measure measure, ByteBuffer byteBuffer) {
    ExpressionAndValuePointers.serialize(measure.getExpressionAndValuePointers(), byteBuffer);
    TypeUtil.serialize(measure.getType(), byteBuffer);
  }

  public static void serialize(Measure measure, DataOutputStream stream) throws IOException {
    ExpressionAndValuePointers.serialize(measure.expressionAndValuePointers, stream);
    TypeUtil.serialize(measure.getType(), stream);
  }

  public static Measure deserialize(ByteBuffer byteBuffer) {
    ExpressionAndValuePointers expressionAndValuePointers =
        ExpressionAndValuePointers.deserialize(byteBuffer);
    Type type = TypeUtil.deserialize(byteBuffer);
    return new Measure(expressionAndValuePointers, type);
  }
}
