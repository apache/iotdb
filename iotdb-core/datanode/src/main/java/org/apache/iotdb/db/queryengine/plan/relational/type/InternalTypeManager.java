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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.type.RowParametricType.ROW;
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
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class InternalTypeManager implements TypeManager {

  private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

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
    types.put(new TypeSignature(TypeEnum.UNKNOWN.name().toLowerCase(Locale.ENGLISH)), UNKNOWN);

    addParametricType(ROW);
  }

  @Override
  public Type getType(TypeSignature signature) throws TypeNotFoundException {
    Type type = types.get(signature);
    if (type == null) {
      return instantiateParametricType(signature);
    }
    return type;
  }

  private void addParametricType(ParametricType parametricType) {
    String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
    if ("ROW".equals(name)) {
      name = "row";
    }
    checkArgument(
        !parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
    parametricTypes.putIfAbsent(name, parametricType);
  }

  private Type instantiateParametricType(TypeSignature signature) {
    List<TypeParameter> parameters = new ArrayList<>();

    for (TypeSignatureParameter parameter : signature.getParameters()) {
      TypeParameter typeParameter = TypeParameter.of(parameter, this);
      parameters.add(typeParameter);
    }

    ParametricType parametricType =
        parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
    if (parametricType == null) {
      throw new TypeNotFoundException(signature);
    }

    Type instantiatedType;
    try {
      instantiatedType = parametricType.createType(this, parameters);
    } catch (IllegalArgumentException e) {
      throw new TypeNotFoundException(signature, e);
    }

    return instantiatedType;
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
      case ROW:
        return TSDataType.BLOB;
      case STRING:
        return TSDataType.STRING;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static Type fromTSDataType(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        return TEXT;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case BOOLEAN:
        return BOOLEAN;
      case UNKNOWN:
        return UNKNOWN;
      case DATE:
        return DATE;
      case TIMESTAMP:
        return TIMESTAMP;
      case BLOB:
        return BLOB;
      case STRING:
        return STRING;
      default:
        throw new IllegalArgumentException();
    }
  }
}
