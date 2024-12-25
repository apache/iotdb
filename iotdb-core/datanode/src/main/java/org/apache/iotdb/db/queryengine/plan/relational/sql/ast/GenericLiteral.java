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

import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.ParsingException;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GenericLiteral extends Literal {

  private final String type;
  private final String value;

  public GenericLiteral(String type, String value) {
    super(null);
    this.type = requireNonNull(type, "type is null");

    if (type.equalsIgnoreCase("X")) {
      // we explicitly disallow "X" as type name, so if the user arrived here,
      // it must be because that he intended to give a binaryLiteral instead, but
      // added whitespace between the X and quote
      throw new ParsingException(
          "Spaces are not allowed between 'X' and the starting quote of a binary literal");
    }
    this.value = requireNonNull(value, "value is null");
  }

  public GenericLiteral(NodeLocation location, String type, String value) {
    super(requireNonNull(location, "location is null"));
    this.type = requireNonNull(type, "type is null");

    if (type.equalsIgnoreCase("X")) {
      // we explicitly disallow "X" as type name, so if the user arrived here,
      // it must be because that he intended to give a binaryLiteral instead, but
      // added whitespace between the X and quote
      throw new ParsingException(
          "Spaces are not allowed between 'X' and the starting quote of a binary literal",
          location);
    }
    this.value = requireNonNull(value, "value is null");
  }

  public String getType() {
    return type;
  }

  public String getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGenericLiteral(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    GenericLiteral other = (GenericLiteral) obj;
    return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    GenericLiteral otherLiteral = (GenericLiteral) other;

    return value.equals(otherLiteral.value) && type.equals(otherLiteral.type);
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.GENERIC_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.type, stream);
    ReadWriteIOUtils.write(this.value, stream);
  }

  public GenericLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.type = ReadWriteIOUtils.readString(byteBuffer);
    this.value = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public Object getTsValue() {
    return new Binary(value.getBytes(StandardCharsets.UTF_8));
  }
}
