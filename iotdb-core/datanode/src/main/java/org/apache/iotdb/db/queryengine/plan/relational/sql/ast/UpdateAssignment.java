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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class UpdateAssignment extends Node {
  private final Expression name;
  private final Expression value;

  public UpdateAssignment(final Expression name, final Expression value) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.value = requireNonNull(value, "value is null");
  }

  public UpdateAssignment(
      final NodeLocation location, final Expression name, final Expression value) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.value = requireNonNull(value, "value is null");
  }

  public Expression getName() {
    return name;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitUpdateAssignment(this, context);
  }

  public void serialize(final ByteBuffer byteBuffer) {
    Expression.serialize(name, byteBuffer);
    Expression.serialize(value, byteBuffer);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    Expression.serialize(name, stream);
    Expression.serialize(value, stream);
  }

  public static UpdateAssignment deserialize(final ByteBuffer buffer) {
    return new UpdateAssignment(Expression.deserialize(buffer), Expression.deserialize(buffer));
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of(name, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final UpdateAssignment other = (UpdateAssignment) obj;
    return Objects.equals(name, other.name) && Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public String toString() {
    return name + " = " + value;
  }
}
