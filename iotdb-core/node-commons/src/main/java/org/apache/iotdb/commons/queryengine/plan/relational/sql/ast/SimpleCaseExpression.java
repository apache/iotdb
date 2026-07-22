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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.i18n.QueryMessages;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SimpleCaseExpression extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SimpleCaseExpression.class);

  private final Expression operand;
  private final List<WhenClause> whenClauses;
  @Nullable private final Expression defaultValue;

  public SimpleCaseExpression(Expression operand, List<WhenClause> whenClauses) {
    super(null);
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.whenClauses =
        ImmutableList.copyOf(
            requireNonNull(whenClauses, QueryMessages.EXCEPTION_WHENCLAUSES_IS_NULL_140535CF));
    this.defaultValue = null;
  }

  public SimpleCaseExpression(
      Expression operand, List<WhenClause> whenClauses, Expression defaultValue) {
    super(null);
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.whenClauses =
        ImmutableList.copyOf(
            requireNonNull(whenClauses, QueryMessages.EXCEPTION_WHENCLAUSES_IS_NULL_140535CF));
    this.defaultValue =
        requireNonNull(defaultValue, QueryMessages.EXCEPTION_DEFAULTVALUE_IS_NULL_B1C8490D);
  }

  public SimpleCaseExpression(
      NodeLocation location, Expression operand, List<WhenClause> whenClauses) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.whenClauses =
        ImmutableList.copyOf(
            requireNonNull(whenClauses, QueryMessages.EXCEPTION_WHENCLAUSES_IS_NULL_140535CF));
    this.defaultValue = null;
  }

  public SimpleCaseExpression(
      NodeLocation location,
      Expression operand,
      List<WhenClause> whenClauses,
      Expression defaultValue) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.whenClauses =
        ImmutableList.copyOf(
            requireNonNull(whenClauses, QueryMessages.EXCEPTION_WHENCLAUSES_IS_NULL_140535CF));
    this.defaultValue =
        requireNonNull(defaultValue, QueryMessages.EXCEPTION_DEFAULTVALUE_IS_NULL_B1C8490D);
  }

  public SimpleCaseExpression(ByteBuffer byteBuffer) {
    super(null);
    int len = ReadWriteIOUtils.readInt(byteBuffer);
    Validate.isTrue(
        len > 0,
        QueryMessages
            .EXCEPTION_THE_LENGTH_OF_SIMPLECASEEXPRESSION_QUOTE_S_WHENCLAUSES_MUST_GREATER_THAN_0_5D2963B9);
    this.operand = Expression.deserialize(byteBuffer);
    this.whenClauses = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      Expression expression = Expression.deserialize(byteBuffer);
      this.whenClauses.add((WhenClause) expression);
    }
    this.defaultValue = Expression.deserialize(byteBuffer);
  }

  public Expression getOperand() {
    return operand;
  }

  public List<WhenClause> getWhenClauses() {
    return whenClauses;
  }

  public Optional<Expression> getDefaultValue() {
    return Optional.ofNullable(defaultValue);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitSimpleCaseExpression(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(operand);
    nodes.addAll(whenClauses);
    if (defaultValue != null) {
      nodes.add(defaultValue);
    }
    return nodes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleCaseExpression that = (SimpleCaseExpression) o;
    return Objects.equals(operand, that.operand)
        && Objects.equals(whenClauses, that.whenClauses)
        && Objects.equals(defaultValue, that.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, whenClauses, defaultValue);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.SIMPLE_CASE;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(this.whenClauses.size(), byteBuffer);
    Expression.serialize(this.operand, byteBuffer);
    getWhenClauses().forEach(child -> Expression.serialize(child, byteBuffer));
    Expression.serialize(getDefaultValue().orElse(new NullLiteral()), byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.whenClauses.size(), stream);
    Expression.serialize(this.operand, stream);
    for (Expression expression : this.getWhenClauses()) {
      Expression.serialize(expression, stream);
    }
    Expression.serialize(getDefaultValue().orElse(new NullLiteral()), stream);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operand);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(whenClauses);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(defaultValue);
    return size;
  }
}
