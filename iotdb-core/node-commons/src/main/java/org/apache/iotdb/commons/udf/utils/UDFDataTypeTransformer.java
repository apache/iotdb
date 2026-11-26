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

import org.apache.iotdb.commons.i18n.SchemaMessages;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.stream.Collectors;

/** Transform between {@link TSDataType} and {@link org.apache.iotdb.udf.api.type.Type} */
public class UDFDataTypeTransformer {

  private UDFDataTypeTransformer() {}

  public static TSDataType transformToTsDataType(Type type) {
    return type == null ? null : TSDataType.getTsDataType(type.getType());
  }

  public static Type transformToUDFDataType(TSDataType tsDataType) {
    if (tsDataType == null) {
      return null;
    }
    try {
      return Type.valueOf(tsDataType.getType());
    } catch (IllegalArgumentException e) {
      throw invalidInput(tsDataType, e);
    }
  }

  public static List<Type> transformToUDFDataTypeList(List<TSDataType> tsDataTypeList) {
    return tsDataTypeList == null
        ? null
        : tsDataTypeList.stream()
            .map(UDFDataTypeTransformer::transformToUDFDataType)
            .collect(Collectors.toList());
  }

  public static Type transformReadTypeToUDFDataType(org.apache.tsfile.read.common.type.Type type) {
    if (type == null) {
      return null;
    }
    try {
      return transformToUDFDataType(TSDataType.valueOf(type.getTypeEnum().name()));
    } catch (IllegalArgumentException e) {
      throw invalidInput(type, e);
    }
  }

  public static org.apache.tsfile.read.common.type.Type transformUDFDataTypeToReadType(Type type) {
    if (type == null) {
      return null;
    }
    return org.apache.tsfile.read.common.type.Type.fromTsDataType(
        TSDataType.getTsDataType(type.getType()));
  }

  private static IllegalArgumentException invalidInput(
      Object type, IllegalArgumentException cause) {
    return new IllegalArgumentException(SchemaMessages.SCHEMA_INVALID_INPUT + type, cause);
  }
}
