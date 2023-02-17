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

package org.apache.iotdb.db.mpp.plan.statement.literal;

import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Literal extends StatementNode {

  public enum LiteralType {
    BOOLEAN,
    DOUBLE,
    LONG,
    STRING,
    NULL
  }

  public static Literal deserialize(ByteBuffer byteBuffer) {
    LiteralType type = LiteralType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    switch (type) {
      case BOOLEAN:
        return new BooleanLiteral(ReadWriteIOUtils.readBool(byteBuffer));
      case DOUBLE:
        return new DoubleLiteral(ReadWriteIOUtils.readDouble(byteBuffer));
      case LONG:
        return new LongLiteral(ReadWriteIOUtils.readLong(byteBuffer));
      case STRING:
        return new StringLiteral(ReadWriteIOUtils.readString(byteBuffer));
      case NULL:
        return new NullLiteral();
      default:
        throw new IllegalArgumentException(String.format("Unknown literal type: %s", type));
    }
  }

  public abstract void serialize(ByteBuffer byteBuffer);

  public abstract void serialize(DataOutputStream stream) throws IOException;

  public abstract boolean isDataTypeConsistency(TSDataType dataType);

  public abstract String getDataTypeString();

  public boolean getBoolean() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  public int getInt() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  public long getLong() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  public float getFloat() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  public double getDouble() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  public Binary getBinary() {
    throw new UnsupportedOperationException(getClass().getName());
  }
}
