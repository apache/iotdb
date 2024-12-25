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

public class ArithmeticBinaryExpression extends Expression {

  public enum Operator {
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    MODULUS("%");
    private final String value;

    Operator(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private final Operator operator;
  private final Expression left;
  private final Expression right;

  public ArithmeticBinaryExpression(Operator operator, Expression left, Expression right) {
    super(null);
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public ArithmeticBinaryExpression(
      @Nonnull NodeLocation location, Operator operator, Expression left, Expression right) {
    super(requireNonNull(location, "location is null"));
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public ArithmeticBinaryExpression(ByteBuffer byteBuffer) {
    super(null);
    this.operator = Operator.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    this.left = Expression.deserialize(byteBuffer);
    this.right = Expression.deserialize(byteBuffer);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.ARITHMETIC_BINARY;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(operator.ordinal(), stream);
    Expression.serialize(left, stream);
    Expression.serialize(right, stream);
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
    return visitor.visitArithmeticBinary(this, context);
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

    ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) o;
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

    return operator == ((ArithmeticBinaryExpression) other).operator;
  }
}
