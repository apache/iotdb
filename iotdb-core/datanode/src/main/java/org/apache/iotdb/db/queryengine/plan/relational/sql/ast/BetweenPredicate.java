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
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class BetweenPredicate extends Expression {

  private final Expression value;
  private final Expression min;
  private final Expression max;

  public BetweenPredicate(Expression value, Expression min, Expression max) {
    super(null);
    requireNonNull(value, "value is null");
    requireNonNull(min, "min is null");
    requireNonNull(max, "max is null");

    this.value = value;
    this.min = min;
    this.max = max;
  }

  public BetweenPredicate(
      @Nonnull NodeLocation location, Expression value, Expression min, Expression max) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(value, "value is null");
    requireNonNull(min, "min is null");
    requireNonNull(max, "max is null");

    this.value = value;
    this.min = min;
    this.max = max;
  }

  public Expression getValue() {
    return value;
  }

  public Expression getMin() {
    return min;
  }

  public Expression getMax() {
    return max;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitBetweenPredicate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(value, min, max);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BetweenPredicate that = (BetweenPredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(min, that.min)
        && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, min, max);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.BETWEEN;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(value, stream);
    Expression.serialize(min, stream);
    Expression.serialize(max, stream);
  }

  public BetweenPredicate(ByteBuffer byteBuffer) {
    super(null);
    this.value = Expression.deserialize(byteBuffer);
    this.min = Expression.deserialize(byteBuffer);
    this.max = Expression.deserialize(byteBuffer);
  }
}
