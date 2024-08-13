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

public class InPredicate extends Expression {

  private final Expression value;
  private final Expression valueList;

  public InPredicate(Expression value, Expression valueList) {
    super(null);
    this.value = requireNonNull(value, "value is null");
    this.valueList = requireNonNull(valueList, "valueList is null");
  }

  public InPredicate(NodeLocation location, Expression value, Expression valueList) {
    super(requireNonNull(location, "location is null"));
    this.value = requireNonNull(value, "value is null");
    this.valueList = requireNonNull(valueList, "valueList is null");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getValueList() {
    return valueList;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInPredicate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(value, valueList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InPredicate that = (InPredicate) o;
    return Objects.equals(value, that.value) && Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, valueList);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.IN_PREDICATE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(value, stream);
    Expression.serialize(valueList, stream);
  }

  public InPredicate(ByteBuffer byteBuffer) {
    super(null);
    this.value = Expression.deserialize(byteBuffer);
    this.valueList = Expression.deserialize(byteBuffer);
  }
}
