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

public class IsNullPredicate extends Expression {

  private final Expression value;

  public IsNullPredicate(Expression value) {
    super(null);
    this.value = requireNonNull(value, "value is null");
  }

  public IsNullPredicate(NodeLocation location, Expression value) {
    super(requireNonNull(location, "location is null"));
    this.value = requireNonNull(value, "value is null");
  }

  public IsNullPredicate(ByteBuffer byteBuffer) {
    super(null);
    this.value = Expression.deserialize(byteBuffer);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.IS_NULL_PREDICATE;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(value, stream);
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIsNullPredicate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IsNullPredicate that = (IsNullPredicate) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
