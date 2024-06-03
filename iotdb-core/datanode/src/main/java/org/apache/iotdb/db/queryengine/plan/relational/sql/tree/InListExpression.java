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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InListExpression extends Expression {
  private final List<Expression> values;

  public InListExpression(List<Expression> values) {
    super(null);
    requireNonNull(values, "values is null");
    checkArgument(!values.isEmpty(), "values cannot be empty");
    this.values = ImmutableList.copyOf(values);
  }

  public InListExpression(NodeLocation location, List<Expression> values) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(values, "values is null");
    checkArgument(!values.isEmpty(), "values cannot be empty");
    this.values = ImmutableList.copyOf(values);
  }

  public InListExpression(ByteBuffer byteBuffer) {
    super(null);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.values = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      values.add(Expression.deserialize(byteBuffer));
    }
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.IN_LIST;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(values.size(), stream);
    for (Expression expression : values) {
      Expression.serialize(expression, stream);
    }
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInListExpression(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InListExpression that = (InListExpression) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
