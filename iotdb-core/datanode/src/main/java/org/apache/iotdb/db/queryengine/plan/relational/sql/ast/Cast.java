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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Cast extends Expression {
  private final Expression expression;
  private final DataType type;
  private final boolean safe;
  private final boolean typeOnly;

  public Cast(Expression expression, DataType type) {
    this(expression, type, false, false);
  }

  public Cast(Expression expression, DataType type, boolean safe) {
    this(expression, type, safe, false);
  }

  public Cast(Expression expression, DataType type, boolean safe, boolean typeOnly) {
    super(null);
    requireNonNull(expression, "expression is null");

    this.expression = expression;
    this.type = type;
    this.safe = safe;
    this.typeOnly = typeOnly;
  }

  public Cast(@Nonnull NodeLocation location, Expression expression, DataType type) {
    this(requireNonNull(location, "location is null"), expression, type, false, false);
  }

  public Cast(@Nonnull NodeLocation location, Expression expression, DataType type, boolean safe) {
    this(requireNonNull(location, "location is null"), expression, type, safe, false);
  }

  private Cast(
      @Nonnull NodeLocation location,
      Expression expression,
      DataType type,
      boolean safe,
      boolean typeOnly) {
    super(location);
    requireNonNull(expression, "expression is null");

    this.expression = expression;
    this.type = type;
    this.safe = safe;
    this.typeOnly = typeOnly;
  }

  public Expression getExpression() {
    return expression;
  }

  public DataType getType() {
    return type;
  }

  public boolean isSafe() {
    return safe;
  }

  public boolean isTypeOnly() {
    return typeOnly;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCast(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(expression, type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Cast cast = (Cast) o;
    return safe == cast.safe
        && typeOnly == cast.typeOnly
        && expression.equals(cast.expression)
        && type.equals(cast.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, type, safe, typeOnly);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    Cast otherCast = (Cast) other;
    return safe == otherCast.safe && typeOnly == otherCast.typeOnly;
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.CAST;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(this.expression, stream);
    serialize(this.type, stream);
    ReadWriteIOUtils.write(this.safe, stream);
    ReadWriteIOUtils.write(this.typeOnly, stream);
  }

  public Cast(ByteBuffer byteBuffer) {
    super(null);
    this.expression = Expression.deserialize(byteBuffer);
    this.type = (DataType) Expression.deserialize(byteBuffer);
    this.safe = ReadWriteIOUtils.readBool(byteBuffer);
    this.typeOnly = ReadWriteIOUtils.readBool(byteBuffer);
  }
}
