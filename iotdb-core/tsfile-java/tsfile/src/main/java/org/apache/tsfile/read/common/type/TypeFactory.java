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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.enums.TSDataType;

public class TypeFactory {

  private TypeFactory() {
    // forbidding instantiation
  }

  public static Type getType(TSDataType tsDataType) {
    switch (tsDataType) {
      case DATE:
        return DateType.getInstance();
      case INT32:
        return IntType.getInstance();
      case INT64:
        return LongType.getInstance();
      case TIMESTAMP:
        return TimestampType.getInstance();
      case FLOAT:
        return FloatType.getInstance();
      case DOUBLE:
        return DoubleType.getInstance();
      case BOOLEAN:
        return BooleanType.getInstance();
      case TEXT:
        return BinaryType.getInstance();
      case STRING:
        return StringType.getInstance();
      case BLOB:
        return BlobType.getInstance();
      default:
        throw new UnsupportedOperationException(
            String.format("Invalid TSDataType for TypeFactory: %s", tsDataType));
    }
  }

  public static Type getType(TypeEnum typeEnum) {
    switch (typeEnum) {
      case INT32:
        return IntType.getInstance();
      case INT64:
        return LongType.getInstance();
      case FLOAT:
        return FloatType.getInstance();
      case DOUBLE:
        return DoubleType.getInstance();
      case BOOLEAN:
        return BooleanType.getInstance();
      case TEXT:
        return BinaryType.getInstance();
      case UNKNOWN:
        return UnknownType.getInstance();
      case DATE:
        return DateType.getInstance();
      case TIMESTAMP:
        return TimestampType.getInstance();
      case BLOB:
        return BlobType.getInstance();
      case STRING:
        return StringType.getInstance();
      default:
        throw new UnsupportedOperationException(
            String.format("Invalid TypeEnum for TypeFactory: %s", typeEnum));
    }
  }
}
