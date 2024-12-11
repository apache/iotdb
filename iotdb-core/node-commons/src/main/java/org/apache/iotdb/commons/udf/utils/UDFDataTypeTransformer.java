/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;

import java.util.List;
import java.util.stream.Collectors;

/** Transform between {@link TSDataType} and {@link org.apache.iotdb.udf.api.type.Type} */
public class UDFDataTypeTransformer {

  private UDFDataTypeTransformer() {}

  public static TSDataType transformToTsDataType(Type type) {
    return type == null ? null : TSDataType.getTsDataType(type.getType());
  }

  public static Type transformToUDFDataType(TSDataType tsDataType) {
    return tsDataType == null ? null : getUDFDataType(tsDataType.getType());
  }

  public static List<Type> transformToUDFDataTypeList(List<TSDataType> tsDataTypeList) {
    return tsDataTypeList == null
        ? null
        : tsDataTypeList.stream()
            .map(UDFDataTypeTransformer::transformToUDFDataType)
            .collect(Collectors.toList());
  }

  public static TSDataType transformReadTypeToTSDataType(
      org.apache.tsfile.read.common.type.Type type) {
    if (type == null) {
      return null;
    }
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case TEXT:
        return TSDataType.TEXT;
      case TIMESTAMP:
        return TSDataType.TIMESTAMP;
      case DATE:
        return TSDataType.DATE;
      case BLOB:
        return TSDataType.BLOB;
      case STRING:
        return TSDataType.STRING;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  public static Type transformReadTypeToUDFDataType(org.apache.tsfile.read.common.type.Type type) {
    if (type == null) {
      return null;
    }
    switch (type.getTypeEnum()) {
      case BOOLEAN:
        return Type.BOOLEAN;
      case INT32:
        return Type.INT32;
      case INT64:
        return Type.INT64;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case TEXT:
        return Type.TEXT;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DATE:
        return Type.DATE;
      case BLOB:
        return Type.BLOB;
      case STRING:
        return Type.STRING;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  public static org.apache.tsfile.read.common.type.Type transformUDFDataTypeToReadType(Type type) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case INT32:
      case DATE:
        return IntType.INT32;
      case INT64:
      case TIMESTAMP:
        return LongType.INT64;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case TEXT:
      case BLOB:
      case STRING:
        return BinaryType.TEXT;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  private static Type getUDFDataType(byte type) {
    switch (type) {
      case 0:
        return Type.BOOLEAN;
      case 1:
        return Type.INT32;
      case 2:
        return Type.INT64;
      case 3:
        return Type.FLOAT;
      case 4:
        return Type.DOUBLE;
      case 5:
        return Type.TEXT;
      case 8:
        return Type.TIMESTAMP;
      case 9:
        return Type.DATE;
      case 10:
        return Type.BLOB;
      case 11:
        return Type.STRING;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }
}
