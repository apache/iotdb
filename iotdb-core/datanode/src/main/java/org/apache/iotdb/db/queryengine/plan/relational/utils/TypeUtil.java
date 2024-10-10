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

import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;

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
        return BooleanType.BOOLEAN;
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
        return DateType.DATE;
      case ROW:
        return RowType.anonymous(subTypes);
      default:
        return UnknownType.UNKNOWN;
    }
  }
}
