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

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TimeDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TimeDurationLiteral extends Literal {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeDurationLiteral.class);

  private final TimeDuration value;

  public TimeDurationLiteral(TimeDuration value) {
    super(null);
    this.value = requireNonNull(value, "value is null");
  }

  public TimeDurationLiteral(NodeLocation location, TimeDuration value) {
    super(requireNonNull(location, "location is null"));
    this.value = requireNonNull(value, "value is null");
  }

  public TimeDuration getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitTimeDurationLiteral(this, context);
  }

  @Override
  public Object getTsValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    TimeDurationLiteral that = (TimeDurationLiteral) obj;
    return Objects.equals(value, that.value);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }
    return Objects.equals(value, ((TimeDurationLiteral) other).value);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.TIME_DURATION_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    value.serialize(stream);
  }

  public TimeDurationLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.value = TimeDuration.deserialize(byteBuffer);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + RamUsageEstimator.sizeOfObject(value);
  }
}
