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

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LikePredicate extends Expression {

  private final Expression value;
  private final Expression pattern;
  @Nullable private final Expression escape;

  public LikePredicate(Expression value, Expression pattern, Expression escape) {
    super(null);
    this.value = requireNonNull(value, "value is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.escape = escape;
  }

  public LikePredicate(NodeLocation location, Expression value, Expression pattern) {
    super(requireNonNull(location, "location is null"));
    this.value = requireNonNull(value, "value is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.escape = null;
  }

  public LikePredicate(Expression value, Expression pattern) {
    super(null);
    this.value = requireNonNull(value, "value is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.escape = null;
  }

  public LikePredicate(
      NodeLocation location, Expression value, Expression pattern, Expression escape) {
    super(requireNonNull(location, "location is null"));
    this.value = requireNonNull(value, "value is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.escape = requireNonNull(escape, "escape is null");
  }

  public LikePredicate(ByteBuffer byteBuffer) {
    super(null);
    this.value = Expression.deserialize(byteBuffer);
    this.pattern = Expression.deserialize(byteBuffer);
    boolean hasEscape = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasEscape) {
      this.escape = Expression.deserialize(byteBuffer);
    } else {
      this.escape = null;
    }
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.LIKE_PREDICATE;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(value, stream);
    Expression.serialize(pattern, stream);
    ReadWriteIOUtils.write(escape != null, stream);
    if (escape != null) {
      Expression.serialize(escape, stream);
    }
  }

  public Expression getValue() {
    return value;
  }

  public Expression getPattern() {
    return pattern;
  }

  public Optional<Expression> getEscape() {
    return Optional.ofNullable(escape);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLikePredicate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> result = ImmutableList.<Node>builder().add(value).add(pattern);

    if (escape != null) {
      result.add(escape);
    }

    return result.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LikePredicate that = (LikePredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(pattern, that.pattern)
        && Objects.equals(escape, that.escape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, pattern, escape);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
