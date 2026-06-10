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

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InListExpression.class);

  private final List<Expression> values;

  public InListExpression(List<Expression> values) {
    super(null);
    requireNonNull(values, QueryMessages.EXCEPTION_VALUES_IS_NULL_F1D7D3D8);
    checkArgument(!values.isEmpty(), QueryMessages.EXCEPTION_VALUES_CANNOT_BE_EMPTY_F18F863D);
    this.values = ImmutableList.copyOf(values);
  }

  public InListExpression(NodeLocation location, List<Expression> values) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    requireNonNull(values, QueryMessages.EXCEPTION_VALUES_IS_NULL_F1D7D3D8);
    checkArgument(!values.isEmpty(), QueryMessages.EXCEPTION_VALUES_CANNOT_BE_EMPTY_F18F863D);
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
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitInListExpression(this, context);
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

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(values);
  }
}
