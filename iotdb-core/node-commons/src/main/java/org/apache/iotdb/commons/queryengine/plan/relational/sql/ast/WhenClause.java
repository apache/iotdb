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
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class WhenClause extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WhenClause.class);

  private final Expression operand;
  private final Expression result;

  public WhenClause(Expression operand, Expression result) {
    super(null);
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.result = requireNonNull(result, QueryMessages.EXCEPTION_RESULT_IS_NULL_031E2F89);
  }

  public WhenClause(NodeLocation location, Expression operand, Expression result) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.operand = requireNonNull(operand, QueryMessages.EXCEPTION_OPERAND_IS_NULL_D0182140);
    this.result = requireNonNull(result, QueryMessages.EXCEPTION_RESULT_IS_NULL_031E2F89);
  }

  public WhenClause(ByteBuffer byteBuffer) {
    super(null);
    this.operand = Expression.deserialize(byteBuffer);
    this.result = Expression.deserialize(byteBuffer);
  }

  public Expression getOperand() {
    return operand;
  }

  public Expression getResult() {
    return result;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitWhenClause(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(operand, result);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WhenClause that = (WhenClause) o;
    return Objects.equals(operand, that.operand) && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, result);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.WHEN_CLAUSE;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(operand, byteBuffer);
    Expression.serialize(result, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(operand, stream);
    Expression.serialize(result, stream);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operand)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(result);
  }
}
