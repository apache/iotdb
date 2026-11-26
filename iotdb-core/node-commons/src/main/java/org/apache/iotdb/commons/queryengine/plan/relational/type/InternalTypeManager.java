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

package org.apache.iotdb.commons.queryengine.plan.relational.type;

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
import static org.apache.tsfile.read.common.type.ObjectType.OBJECT;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

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
    types.put(new TypeSignature(TypeEnum.OBJECT.name().toLowerCase(Locale.ENGLISH)), OBJECT);
    types.put(new TypeSignature(TypeEnum.DATE.name().toLowerCase(Locale.ENGLISH)), DATE);
    types.put(new TypeSignature(TypeEnum.TIMESTAMP.name().toLowerCase(Locale.ENGLISH)), TIMESTAMP);
    types.put(new TypeSignature(TypeEnum.UNKNOWN.name().toLowerCase(Locale.ENGLISH)), UNKNOWN);
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
    final TypeEnum typeEnum = type.getTypeEnum();
    // TSDataType has no ROW counterpart, so preserve its existing binary representation.
    if (typeEnum == TypeEnum.ROW) {
      return TSDataType.BLOB;
    }
    if (typeEnum == TypeEnum.VECTOR) {
      throw new IllegalArgumentException();
    }
    return TSDataType.valueOf(typeEnum.name());
  }

  public static Type fromTSDataType(TSDataType dataType) {
    if (dataType == TSDataType.VECTOR) {
      throw new IllegalArgumentException();
    }
    return Type.fromTsDataType(dataType);
  }
}
