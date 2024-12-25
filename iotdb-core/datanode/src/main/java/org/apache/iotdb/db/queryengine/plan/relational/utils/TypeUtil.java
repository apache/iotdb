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
package org.apache.iotdb.db.queryengine.plan.relational.utils;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.FlatHashStrategy;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.HashStrategy;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.XxHash64;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.XxHash64.FALSE_XX_HASH;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.XxHash64.TRUE_XX_HASH;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.utils.BytesUtils.bytesToDouble;
import static org.apache.tsfile.utils.BytesUtils.bytesToFloat;
import static org.apache.tsfile.utils.BytesUtils.bytesToInt;
import static org.apache.tsfile.utils.BytesUtils.bytesToLongFromOffset;
import static org.apache.tsfile.utils.BytesUtils.doubleToBytes;
import static org.apache.tsfile.utils.BytesUtils.floatToBytes;
import static org.apache.tsfile.utils.BytesUtils.intToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

public class TypeUtil {

  public static void serialize(Type type, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(type.getTypeEnum().ordinal(), byteBuffer);
    List<Type> typeParameters = type.getTypeParameters();
    ReadWriteIOUtils.write(typeParameters.size(), byteBuffer);
    for (Type typeParameter : typeParameters) {
      ReadWriteIOUtils.write(typeParameter.getTypeEnum().ordinal(), byteBuffer);
    }
  }

  public static void serialize(Type type, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type.getTypeEnum().ordinal(), stream);
    List<Type> typeParameters = type.getTypeParameters();
    ReadWriteIOUtils.write(typeParameters.size(), stream);
    for (Type typeParameter : typeParameters) {
      ReadWriteIOUtils.write(typeParameter.getTypeEnum().ordinal(), stream);
    }
  }

  public static Type deserialize(ByteBuffer byteBuffer) {
    TypeEnum typeEnum = TypeEnum.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Type> typeParameters = new ArrayList<>(size);
    while (size-- > 0) {
      typeParameters.add(
          getType(
              TypeEnum.values()[ReadWriteIOUtils.readInt(byteBuffer)], Collections.emptyList()));
    }
    return getType(typeEnum, typeParameters);
  }

  public static Type getType(TypeEnum typeEnum, List<Type> subTypes) {
    switch (typeEnum) {
      case BOOLEAN:
        return BOOLEAN;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case TEXT:
        return BinaryType.TEXT;
      case STRING:
        return StringType.STRING;
      case BLOB:
        return BlobType.BLOB;
      case TIMESTAMP:
        return TimestampType.TIMESTAMP;
      case DATE:
        return DATE;
      case ROW:
        return RowType.anonymous(subTypes);
      default:
        return UnknownType.UNKNOWN;
    }
  }

  // TODO move these methods into each Type to avoid branch miss
  public static boolean isFlatVariableWidth(Type type) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DATE:
        return false;
      case TEXT:
      case STRING:
      case BLOB:
        return true;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static int getFlatFixedSize(Type type) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return 1;
      case INT32:
      case DATE:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case TEXT:
      case STRING:
      case BLOB:
        return 16;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static int getFlatVariableWidthSize(Type type, Column column, int position) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DATE:
        return 0;
      case TEXT:
      case STRING:
      case BLOB:
        return column.isNull(position) ? 0 : column.getBinary(position).getLength();
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static int getFlatVariableWidthSize(Type type, Column column, int[] position) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DATE:
        return 0;
      case TEXT:
      case STRING:
      case BLOB:
        int result = 0;
        for (int i = 0; i < position.length; i++) {
          if (!column.isNull(i)) {
            result += column.getBinary(position[i]).getLength();
          }
        }
        return result;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static void readFlat(
      Type type,
      byte[] fixedChunk,
      int fixedOffset,
      byte[] variableChunk,
      ColumnBuilder columnBuilder) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        columnBuilder.writeBoolean(fixedChunk[fixedOffset] != 0);
        break;
      case INT32:
      case DATE:
        columnBuilder.writeInt(bytesToInt(fixedChunk, fixedOffset));
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(bytesToLongFromOffset(fixedChunk, Long.BYTES, fixedOffset));
        break;
      case FLOAT:
        columnBuilder.writeFloat(bytesToFloat(fixedChunk, fixedOffset));
        break;
      case DOUBLE:
        columnBuilder.writeDouble(bytesToDouble(fixedChunk, fixedOffset));
        break;
      case TEXT:
      case STRING:
      case BLOB:
        int length = bytesToInt(fixedChunk, fixedOffset);
        byte[] result = new byte[length];
        if (length <= 12) {
          System.arraycopy(fixedChunk, fixedOffset + Integer.BYTES, result, 0, length);
          columnBuilder.writeBinary(new Binary(result));
        } else {
          int variableSizeOffset = bytesToInt(fixedChunk, fixedOffset + Integer.BYTES + Long.BYTES);
          System.arraycopy(variableChunk, variableSizeOffset, result, 0, length);
          columnBuilder.writeBinary(new Binary(result));
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static void writeFlat(
      Type type,
      Column column,
      int position,
      byte[] fixedChunk,
      int fixedOffset,
      byte[] variableChunk,
      int variableOffset) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        fixedChunk[fixedOffset] = (byte) (column.getBoolean(position) ? 1 : 0);
        break;
      case INT32:
      case DATE:
        intToBytes(column.getInt(position), fixedChunk, fixedOffset);
        break;
      case INT64:
      case TIMESTAMP:
        longToBytes(column.getLong(position), fixedChunk, fixedOffset);
        break;
      case FLOAT:
        floatToBytes(column.getFloat(position), fixedChunk, fixedOffset);
        break;
      case DOUBLE:
        doubleToBytes(column.getDouble(position), fixedChunk, fixedOffset);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        byte[] value = column.getBinary(position).getValues();
        intToBytes(value.length, fixedChunk, fixedOffset);
        if (value.length <= 12) {
          System.arraycopy(value, 0, fixedChunk, fixedOffset + Integer.BYTES, value.length);
        } else {
          intToBytes(variableOffset, fixedChunk, fixedOffset + Integer.BYTES + Long.BYTES);
          System.arraycopy(value, 0, variableChunk, variableOffset, value.length);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static boolean notDistinctFrom(
      Type type,
      byte[] fixedChunk,
      int fixedOffset,
      byte[] variableChunk,
      Column column,
      int position) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return (fixedChunk[fixedOffset] != 0) == column.getBoolean(position);
      case INT32:
      case DATE:
        return bytesToInt(fixedChunk, fixedOffset) == column.getInt(position);
      case INT64:
      case TIMESTAMP:
        return bytesToLongFromOffset(fixedChunk, Long.BYTES, fixedOffset)
            == column.getLong(position);
      case FLOAT:
        return bytesToFloat(fixedChunk, fixedOffset) == column.getFloat(position);
      case DOUBLE:
        return bytesToDouble(fixedChunk, fixedOffset) == column.getDouble(position);
      case TEXT:
      case STRING:
      case BLOB:
        int leftLength = bytesToInt(fixedChunk, fixedOffset);
        byte[] leftValue = new byte[leftLength];
        byte[] rightValue = column.getBinary(position).getValues();

        if (leftLength != rightValue.length) {
          return false;
        }

        if (leftLength <= 12) {
          System.arraycopy(fixedChunk, fixedOffset + Integer.BYTES, leftValue, 0, leftLength);
        } else {
          int variableSizeOffset = bytesToInt(fixedChunk, fixedOffset + Integer.BYTES + Long.BYTES);
          System.arraycopy(variableChunk, variableSizeOffset, leftValue, 0, leftLength);
        }
        return Arrays.equals(leftValue, rightValue);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /*public static long hash(Type type, byte[] fixedChunk, int fixedOffset, byte[] variableChunk) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return fixedChunk[fixedOffset] != 0 ? TRUE_XX_HASH : FALSE_XX_HASH;
      case INT32:
      case DATE:
        return rotateLeft(((long) bytesToInt(fixedChunk, fixedOffset)) * 0xC2B2AE3D27D4EB4FL, 31)
            * 0x9E3779B185EBCA87L;
      case INT64:
      case TIMESTAMP:
        // xxHash64 mix
        return rotateLeft(
                bytesToLongFromOffset(fixedChunk, Long.BYTES, fixedOffset) * 0xC2B2AE3D27D4EB4FL,
                31)
            * 0x9E3779B185EBCA87L;
      case FLOAT:
        float value = bytesToFloat(fixedChunk, fixedOffset);
        if (value == 0) {
          return 0;
        }
        return rotateLeft(((long) floatToIntBits(value) * 0xC2B2AE3D27D4EB4FL), 31)
            * 0x9E3779B185EBCA87L;
      case DOUBLE:
        double value1 = bytesToDouble(fixedChunk, fixedOffset);
        if (value1 == 0) {
          return 0;
        }
        return rotateLeft((doubleToLongBits(value1) * 0xC2B2AE3D27D4EB4FL), 31)
            * 0x9E3779B185EBCA87L;
      case TEXT:
      case STRING:
      case BLOB:
        int length = bytesToInt(fixedChunk, fixedOffset);
        byte[] values = new byte[length];

        if (length <= 12) {
          System.arraycopy(fixedChunk, fixedOffset + Integer.BYTES, values, 0, length);
        } else {
          int variableSizeOffset = bytesToInt(fixedChunk, fixedOffset + Integer.BYTES + Long.BYTES);
          System.arraycopy(variableChunk, variableSizeOffset, values, 0, length);
        }
        return XxHash64.hash(values);
      default:
        throw new UnsupportedOperationException();
    }
  }*/

  public static long hash(Type type, byte[] fixedChunk, int fixedOffset, byte[] variableChunk) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return fixedChunk[fixedOffset] != 0 ? TRUE_XX_HASH : FALSE_XX_HASH;
      case INT32:
      case DATE:
        return XxHash64.hash(bytesToInt(fixedChunk, fixedOffset));
      case INT64:
      case TIMESTAMP:
        // xxHash64 mix

        return XxHash64.hash(bytesToLongFromOffset(fixedChunk, Long.BYTES, fixedOffset));
      case FLOAT:
        float value = bytesToFloat(fixedChunk, fixedOffset);
        if (value == 0) {
          return 0;
        }
        return XxHash64.hash(floatToIntBits(value));
      case DOUBLE:
        double value1 = bytesToDouble(fixedChunk, fixedOffset);
        if (value1 == 0) {
          return 0;
        }
        return XxHash64.hash(doubleToLongBits(value1));
      case TEXT:
      case STRING:
      case BLOB:
        int length = bytesToInt(fixedChunk, fixedOffset);
        byte[] values = new byte[length];

        if (length <= 12) {
          System.arraycopy(fixedChunk, fixedOffset + Integer.BYTES, values, 0, length);
        } else {
          int variableSizeOffset = bytesToInt(fixedChunk, fixedOffset + Integer.BYTES + Long.BYTES);
          System.arraycopy(variableChunk, variableSizeOffset, values, 0, length);
        }
        return XxHash64.hash(values);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /*public static long hash(Type type, Column column, int position) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return column.getBoolean(position) ? TRUE_XX_HASH : FALSE_XX_HASH;
      case INT32:
      case DATE:
        return rotateLeft(((long) column.getInt(position)) * 0xC2B2AE3D27D4EB4FL, 31)
            * 0x9E3779B185EBCA87L;
      case INT64:
      case TIMESTAMP:
        // xxHash64 mix
        return rotateLeft(column.getLong(position) * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
      case FLOAT:
        float value = column.getFloat(position);
        if (value == 0) {
          return 0;
        }
        return rotateLeft(((long) floatToIntBits(value) * 0xC2B2AE3D27D4EB4FL), 31)
            * 0x9E3779B185EBCA87L;
      case DOUBLE:
        double value1 = column.getDouble(position);
        if (value1 == 0) {
          return 0;
        }
        return rotateLeft((doubleToLongBits(value1) * 0xC2B2AE3D27D4EB4FL), 31)
            * 0x9E3779B185EBCA87L;
      case TEXT:
      case STRING:
      case BLOB:
        return XxHash64.hash(column.getBinary(position).getValues());
      default:
        throw new UnsupportedOperationException();
    }
  }*/

  public static long hash(Type type, Column column, int position) {
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return column.getBoolean(position) ? TRUE_XX_HASH : FALSE_XX_HASH;
      case INT32:
      case DATE:
        return XxHash64.hash(column.getInt(position));
      case INT64:
      case TIMESTAMP:
        // xxHash64 mix
        return XxHash64.hash(column.getLong(position));
      case FLOAT:
        float value = column.getFloat(position);
        if (value == 0) {
          return 0;
        }
        return XxHash64.hash(floatToIntBits(column.getFloat(position)));
      case DOUBLE:
        double value1 = column.getDouble(position);
        if (value1 == 0) {
          return 0;
        }
        return XxHash64.hash(doubleToLongBits(column.getDouble(position)));
      case TEXT:
      case STRING:
      case BLOB:
        return XxHash64.hash(column.getBinary(position).getValues());
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static FlatHashStrategy getFlatHashStrategy(List<Type> hashTypes) {
    return new HashStrategy(hashTypes);
  }
}
