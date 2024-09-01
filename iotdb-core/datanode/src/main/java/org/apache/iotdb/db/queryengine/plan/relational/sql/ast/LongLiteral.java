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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class LongLiteral extends Literal {

  private final String value;
  private final long parsedValue;

  public LongLiteral(String value) {
    super(null);
    try {
      this.value = value;
      this.parsedValue = parse(value);
    } catch (NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + value);
    }
  }

  public LongLiteral(NodeLocation location, String value) {
    super(requireNonNull(location, "location is null"));
    try {
      this.value = value;
      this.parsedValue = parse(value);
    } catch (NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + value, location);
    }
  }

  public String getValue() {
    return value;
  }

  public long getParsedValue() {
    return parsedValue;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLongLiteral(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongLiteral that = (LongLiteral) o;

    if (parsedValue != that.parsedValue) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (parsedValue ^ (parsedValue >>> 32));
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return parsedValue == ((LongLiteral) other).parsedValue;
  }

  private static long parse(String value) {
    value = value.replace("_", "");

    if (value.startsWith("0x") || value.startsWith("0X")) {
      return Long.parseLong(value.substring(2), 16);
    } else if (value.startsWith("0b") || value.startsWith("0B")) {
      return Long.parseLong(value.substring(2), 2);
    } else if (value.startsWith("0o") || value.startsWith("0O")) {
      return Long.parseLong(value.substring(2), 8);
    } else {
      return Long.parseLong(value);
    }
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.LONG_LITERAL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.value, stream);
    ReadWriteIOUtils.write(this.parsedValue, stream);
  }

  public LongLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.value = ReadWriteIOUtils.readString(byteBuffer);
    this.parsedValue = ReadWriteIOUtils.readLong(byteBuffer);
  }

  @Override
  public Object getTsValue() {
    return parsedValue;
  }
}
