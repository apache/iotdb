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

package org.apache.iotdb.db.queryengine.plan.statement.literal;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import static org.apache.tsfile.utils.DateUtils.parseIntToLocalDate;

public class LongLiteral extends Literal {
  private final long value;

  public LongLiteral(String value) {
    this.value = Long.parseLong(value);
  }

  public LongLiteral(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(LiteralType.LONG.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(LiteralType.LONG.ordinal(), stream);
    ReadWriteIOUtils.write(value, stream);
  }

  @Override
  public boolean isDataTypeConsistency(TSDataType dataType) {
    if (dataType == TSDataType.INT32 || dataType == TSDataType.DATE) {
      try {
        Math.toIntExact(value);
        if (dataType == TSDataType.INT32) {
          return true;
        }
      } catch (ArithmeticException e) {
        return false;
      }

      // check date type
      try {
        parseIntToLocalDate((int) value);
        return true;
      } catch (DateTimeParseException e) {
        return false;
      }
    }
    return dataType == TSDataType.INT64
        || dataType == TSDataType.FLOAT
        || dataType == TSDataType.DOUBLE
        || dataType == TSDataType.TEXT
        || dataType == TSDataType.STRING
        || dataType == TSDataType.TIMESTAMP;
  }

  @Override
  public String getDataTypeString() {
    return TSDataType.INT64.toString();
  }

  @Override
  public int getInt() {
    return Math.toIntExact(value);
  }

  @Override
  public int getDate() {
    return Math.toIntExact(value);
  }

  @Override
  public long getLong() {
    return getValue();
  }

  @Override
  public float getFloat() {
    return getValue();
  }

  @Override
  public double getDouble() {
    return getValue();
  }

  @Override
  public Binary getBinary() {
    return new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET);
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
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
