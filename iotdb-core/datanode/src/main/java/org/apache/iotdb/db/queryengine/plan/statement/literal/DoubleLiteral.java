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
import java.util.Objects;

public class DoubleLiteral extends Literal {
  private final double value;

  public DoubleLiteral(String value) {
    this.value = Double.parseDouble(value);
  }

  public DoubleLiteral(double value) {
    this.value = value;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(LiteralType.DOUBLE.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(LiteralType.DOUBLE.ordinal(), stream);
    ReadWriteIOUtils.write(value, stream);
  }

  @Override
  public boolean isDataTypeConsistency(TSDataType dataType) {
    return dataType == TSDataType.FLOAT
        || dataType == TSDataType.DOUBLE
        || dataType == TSDataType.TEXT
        || dataType == TSDataType.STRING;
  }

  @Override
  public String getDataTypeString() {
    return TSDataType.DOUBLE.toString();
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
    return Double.compare(that.value, value) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public double getDouble() {
    return value;
  }

  @Override
  public float getFloat() {
    return (float) value;
  }

  @Override
  public Binary getBinary() {
    return new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET);
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
