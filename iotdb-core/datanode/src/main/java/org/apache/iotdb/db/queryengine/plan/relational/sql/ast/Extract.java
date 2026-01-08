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
import com.google.errorprone.annotations.Immutable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class Extract extends Expression {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Extract.class);

  private final Expression expression;
  private final Field field;

  public enum Field {
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    DOW,
    DAY_OF_YEAR,
    DOY,
    HOUR,
    MINUTE,
    SECOND,
    MS,
    US,
    NS
  }

  public Extract(Expression expression, Field field) {
    this(null, expression, field);
  }

  public Extract(NodeLocation location, Expression expression, Field field) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(field, "field is null");

    this.expression = expression;
    this.field = field;
  }

  public Extract(ByteBuffer byteBuffer) {
    super(null);
    expression = deserialize(byteBuffer);
    field = Field.values()[ReadWriteIOUtils.readInt(byteBuffer)];
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    serialize(expression, byteBuffer);
    ReadWriteIOUtils.write(field.ordinal(), byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    serialize(expression, stream);
    ReadWriteIOUtils.write(field.ordinal(), stream);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.EXTRACT;
  }

  public Expression getExpression() {
    return expression;
  }

  public Field getField() {
    return field;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExtract(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(expression);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Extract that = (Extract) o;
    return Objects.equals(expression, that.expression) && (field == that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, field);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    Extract otherExtract = (Extract) other;
    return field.equals(otherExtract.field);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(expression);
  }
}
