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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class DoubleLiteral extends Literal {

  private final double value;

  public DoubleLiteral(String value) {
    super(null);
    this.value = Double.parseDouble(requireNonNull(value, "value is null"));
  }

  public DoubleLiteral(double value) {
    super(null);
    this.value = value;
  }

  public DoubleLiteral(NodeLocation location, String value) {
    super(requireNonNull(location, "location is null"));
    this.value = Double.parseDouble(requireNonNull(value, "value is null"));
  }

  public double getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDoubleLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DoubleLiteral that = (DoubleLiteral) o;

    if (Double.compare(that.value, value) != 0) {
      return false;
    }

    return true;
  }

  @SuppressWarnings("UnaryPlus")
  @Override
  public int hashCode() {
    long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
    return (int) (temp ^ (temp >>> 32));
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return value == ((DoubleLiteral) other).value;
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.DOUBLE_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.value, stream);
  }

  public DoubleLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.value = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  public Object getTsValue() {
    return value;
  }
}
