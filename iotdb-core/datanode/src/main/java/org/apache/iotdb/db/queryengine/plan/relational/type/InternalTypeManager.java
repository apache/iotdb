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

package org.apache.iotdb.db.queryengine.plan.relational.type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class InternalTypeManager implements TypeManager {

  private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();

  public InternalTypeManager() {
    types.put(new TypeSignature(TypeEnum.DOUBLE.name().toLowerCase(Locale.ENGLISH)), DOUBLE);
    types.put(new TypeSignature(TypeEnum.FLOAT.name().toLowerCase(Locale.ENGLISH)), FLOAT);
    types.put(new TypeSignature(TypeEnum.INT64.name().toLowerCase(Locale.ENGLISH)), INT64);
    types.put(new TypeSignature(TypeEnum.INT32.name().toLowerCase(Locale.ENGLISH)), INT32);
    types.put(new TypeSignature(TypeEnum.BOOLEAN.name().toLowerCase(Locale.ENGLISH)), BOOLEAN);
    types.put(new TypeSignature(TypeEnum.TEXT.name().toLowerCase(Locale.ENGLISH)), TEXT);
    types.put(new TypeSignature(TypeEnum.STRING.name().toLowerCase(Locale.ENGLISH)), STRING);
    types.put(new TypeSignature(TypeEnum.BLOB.name().toLowerCase(Locale.ENGLISH)), BLOB);
    types.put(new TypeSignature(TypeEnum.DATE.name().toLowerCase(Locale.ENGLISH)), DATE);
    types.put(new TypeSignature(TypeEnum.TIMESTAMP.name().toLowerCase(Locale.ENGLISH)), TIMESTAMP);
  }

  @Override
  public Type getType(TypeSignature signature) throws TypeNotFoundException {
    Type type = types.get(signature);
    if (type == null) {
      throw new TypeNotFoundException(signature);
    }
    return type;
  }

  @Override
  public Type fromSqlType(String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getType(TypeId id) {
    throw new UnsupportedOperationException();
  }

  public static TSDataType getTSDataType(Type type) {
    if (type == null) {
      return null;
    }
    TypeEnum typeEnum = type.getTypeEnum();
    switch (typeEnum) {
      case TEXT:
        return TSDataType.TEXT;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case UNKNOWN:
        return TSDataType.UNKNOWN;
      case DATE:
        return TSDataType.DATE;
      case TIMESTAMP:
        return TSDataType.TIMESTAMP;
      case BLOB:
        return TSDataType.BLOB;
      case STRING:
        return TSDataType.STRING;
      default:
        throw new IllegalArgumentException();
    }
  }
}
