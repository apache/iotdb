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

import com.google.common.base.CharMatcher;
import com.google.common.io.BaseEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BinaryLiteral extends Literal {
  // the grammar could possibly include whitespace in the value it passes to us
  private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
  private static final CharMatcher HEX_DIGIT_MATCHER =
      CharMatcher.inRange('A', 'F').or(CharMatcher.inRange('0', '9')).precomputed();

  private final byte[] value;

  public BinaryLiteral(String value) {
    super(null);
    requireNonNull(value, "value is null");
    String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
    if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
      throw new ParsingException("Binary literal can only contain hexadecimal digits");
    }
    if (hexString.length() % 2 != 0) {
      throw new ParsingException("Binary literal must contain an even number of digits");
    }
    this.value = BaseEncoding.base16().decode(hexString);
  }

  public BinaryLiteral(byte[] value) {
    super(null);
    requireNonNull(value, "value is null");
    this.value = value;
  }

  public BinaryLiteral(NodeLocation location, String value) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(value, "value is null");
    String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
    if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
      throw new ParsingException("Binary literal can only contain hexadecimal digits", location);
    }
    if (hexString.length() % 2 != 0) {
      throw new ParsingException("Binary literal must contain an even number of digits", location);
    }
    this.value = BaseEncoding.base16().decode(hexString);
  }

  /** Return the valued as a hex-formatted string with upper-case characters */
  public String toHexString() {
    return BaseEncoding.base16().encode(value);
  }

  public byte[] getValue() {
    return value.clone();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitBinaryLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BinaryLiteral that = (BinaryLiteral) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return Arrays.equals(value, ((BinaryLiteral) other).value);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.BINARY_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.value.length, stream);
    for (byte b : value) {
      ReadWriteIOUtils.write(b, stream);
    }
  }

  public BinaryLiteral(ByteBuffer byteBuffer) {
    super(null);
    int length = ReadWriteIOUtils.readInt(byteBuffer);
    this.value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = ReadWriteIOUtils.readByte(byteBuffer);
    }
  }

  @Override
  public Object getTsValue() {
    return new Binary(value);
  }
}
