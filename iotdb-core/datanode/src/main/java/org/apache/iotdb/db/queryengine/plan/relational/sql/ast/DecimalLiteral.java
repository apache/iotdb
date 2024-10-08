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

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DecimalLiteral extends Literal {

  private final String value;

  public DecimalLiteral(String value) {
    super(null);
    this.value = requireNonNull(value, "value is null");
  }

  public DecimalLiteral(NodeLocation location, String value) {
    super(requireNonNull(location, "location is null"));

    this.value = requireNonNull(value, "value is null");
  }

  public String getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDecimalLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DecimalLiteral that = (DecimalLiteral) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    DecimalLiteral otherLiteral = (DecimalLiteral) other;
    return value.equals(otherLiteral.value);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.DECIMAL_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.value, stream);
  }

  public DecimalLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.value = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public Object getTsValue() {
    return new Binary(value.getBytes(StandardCharsets.UTF_8));
  }
}
