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

public class WhenClause extends Expression {

  private final Expression operand;
  private final Expression result;

  public WhenClause(Expression operand, Expression result) {
    super(null);
    this.operand = requireNonNull(operand, "operand is null");
    this.result = requireNonNull(result, "result is null");
  }

  public WhenClause(NodeLocation location, Expression operand, Expression result) {
    super(requireNonNull(location, "location is null"));
    this.operand = requireNonNull(operand, "operand is null");
    this.result = requireNonNull(result, "result is null");
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
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWhenClause(this, context);
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
}
