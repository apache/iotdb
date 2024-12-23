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

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GenericDataType extends DataType {

  private final Identifier name;
  private final List<DataTypeParameter> arguments;

  public GenericDataType(Identifier name, List<DataTypeParameter> arguments) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public GenericDataType(
      @Nonnull NodeLocation location, Identifier name, List<DataTypeParameter> arguments) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public Identifier getName() {
    return name;
  }

  public List<DataTypeParameter> getArguments() {
    return arguments;
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().add(name).addAll(arguments).build();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGenericDataType(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericDataType that = (GenericDataType) o;
    return name.equals(that.name) && arguments.equals(that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.GENERIC_DATA_TYPE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    serialize(name, stream);
    // ReadWriteIOUtils.write(arguments.size(), stream);
  }

  public GenericDataType(ByteBuffer byteBuffer) {
    super(null);
    this.name = (Identifier) deserialize(byteBuffer);
    // arguments are always empty now
    this.arguments = Collections.emptyList();
  }
}
