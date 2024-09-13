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

public class ArithmeticUnaryExpression extends Expression {

  public enum Sign {
    PLUS,
    MINUS
  }

  private final Expression value;
  private final Sign sign;

  public ArithmeticUnaryExpression(NodeLocation location, Sign sign, Expression value) {
    super(location);
    requireNonNull(value, "value is null");
    requireNonNull(sign, "sign is null");

    this.value = value;
    this.sign = sign;
  }

  public ArithmeticUnaryExpression(Sign sign, Expression value) {
    super(null);
    requireNonNull(value, "value is null");
    requireNonNull(sign, "sign is null");

    this.value = value;
    this.sign = sign;
  }

  public ArithmeticUnaryExpression(ByteBuffer byteBuffer) {
    super(null);
    this.value = Expression.deserialize(byteBuffer);
    this.sign = Sign.values()[ReadWriteIOUtils.readInt(byteBuffer)];
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.ARITHMETIC_UNARY;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(value, stream);
    ReadWriteIOUtils.write(sign.ordinal(), stream);
  }

  public static ArithmeticUnaryExpression positive(
      @Nonnull NodeLocation location, Expression value) {
    return new ArithmeticUnaryExpression(
        requireNonNull(location, "location is null"), Sign.PLUS, value);
  }

  public static ArithmeticUnaryExpression negative(
      @Nonnull NodeLocation location, Expression value) {
    return new ArithmeticUnaryExpression(
        requireNonNull(location, "location is null"), Sign.MINUS, value);
  }

  public static ArithmeticUnaryExpression positive(Expression value) {
    return new ArithmeticUnaryExpression(null, Sign.PLUS, value);
  }

  public static ArithmeticUnaryExpression negative(Expression value) {
    return new ArithmeticUnaryExpression(null, Sign.MINUS, value);
  }

  public Expression getValue() {
    return value;
  }

  public Sign getSign() {
    return sign;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitArithmeticUnary(this, context);
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

    ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;
    return Objects.equals(value, that.value) && (sign == that.sign);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, sign);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return sign == ((ArithmeticUnaryExpression) other).sign;
  }
}
