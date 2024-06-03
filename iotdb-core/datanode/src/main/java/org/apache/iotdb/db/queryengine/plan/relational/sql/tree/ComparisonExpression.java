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

package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ComparisonExpression extends Expression {

  public enum Operator {
    EQUAL("="),
    NOT_EQUAL("<>"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    IS_DISTINCT_FROM("IS DISTINCT FROM");

    private final String value;

    Operator(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public Operator flip() {
      switch (this) {
        case EQUAL:
          return EQUAL;
        case NOT_EQUAL:
          return NOT_EQUAL;
        case LESS_THAN:
          return GREATER_THAN;
        case LESS_THAN_OR_EQUAL:
          return GREATER_THAN_OR_EQUAL;
        case GREATER_THAN:
          return LESS_THAN;
        case GREATER_THAN_OR_EQUAL:
          return LESS_THAN_OR_EQUAL;
        case IS_DISTINCT_FROM:
          return IS_DISTINCT_FROM;
      }
      throw new IllegalArgumentException("Unsupported comparison: " + this);
    }

    public Operator negate() {
      switch (this) {
        case EQUAL:
          return NOT_EQUAL;
        case NOT_EQUAL:
          return EQUAL;
        case LESS_THAN:
          return GREATER_THAN_OR_EQUAL;
        case LESS_THAN_OR_EQUAL:
          return GREATER_THAN;
        case GREATER_THAN:
          return LESS_THAN_OR_EQUAL;
        case GREATER_THAN_OR_EQUAL:
          return LESS_THAN;
        case IS_DISTINCT_FROM:
          // Cannot negate
          break;
      }
      throw new IllegalArgumentException("Unsupported comparison: " + this);
    }
  }

  private final Operator operator;
  private final Expression left;
  private final Expression right;

  public ComparisonExpression(Operator operator, Expression left, Expression right) {
    super(null);
    requireNonNull(operator, "operator is null");
    requireNonNull(left, "left is null");
    requireNonNull(right, "right is null");

    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public ComparisonExpression(
      @Nonnull NodeLocation location, Operator operator, Expression left, Expression right) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(operator, "operator is null");
    requireNonNull(left, "left is null");
    requireNonNull(right, "right is null");

    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitComparisonExpression(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ComparisonExpression that = (ComparisonExpression) o;
    return (operator == that.operator)
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operator, left, right);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return operator == ((ComparisonExpression) other).operator;
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.COMPARISON;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(operator.ordinal(), stream);
    serialize(left, stream);
    serialize(right, stream);
  }

  public ComparisonExpression(ByteBuffer byteBuffer) {
    super(null);
    operator = Operator.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    left = deserialize(byteBuffer);
    right = deserialize(byteBuffer);
  }
}
