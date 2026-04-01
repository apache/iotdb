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

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class FloatLiteral extends Literal {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FloatLiteral.class);

  private final float value;

  public FloatLiteral(String value) {
    super(null);
    this.value = Float.parseFloat(requireNonNull(value, "value is null"));
  }

  public FloatLiteral(float value) {
    super(null);
    this.value = value;
  }

  public FloatLiteral(NodeLocation location, String value) {
    super(null);
    throw new SemanticException("Currently the FloatLiteral cannot be created from NodeLocation");
  }

  public float getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFloatLiteral(this, context);
  }

  @SuppressWarnings("UnaryPlus")
  @Override
  public int hashCode() {
    return value != +0.0f ? Float.floatToIntBits(value) : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FloatLiteral that = (FloatLiteral) o;
    return Float.compare(that.value, value) == 0;
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return value == ((FloatLiteral) other).value;
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.FLOAT_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.value, stream);
  }

  public FloatLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.value = ReadWriteIOUtils.readFloat(byteBuffer);
  }

  @Override
  public Object getTsValue() {
    return value;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
  }
}
