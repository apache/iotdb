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

package org.apache.iotdb.udf.api.type;

import org.apache.tsfile.utils.Binary;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

/** A substitution class for TsDataType in UDF APIs. */
public enum Type {
  /* BOOLEAN */
  BOOLEAN((byte) 0),

  /* INT32 */
  INT32((byte) 1),

  /* INT64 */
  INT64((byte) 2),

  /* FLOAT */
  FLOAT((byte) 3),

  /* DOUBLE */
  DOUBLE((byte) 4),

  /* TEXT */
  TEXT((byte) 5),

  /* TsDataType.Vector and TsDataType.UNKNOWN are inner types of TsFile-module, which should not be supported in UDF APIs. To be consistent with TsDataType, the next value starts with 8 */
  /* TIMESTAMP */
  TIMESTAMP((byte) 8),

  /* DATE */
  DATE((byte) 9),

  /* BLOB */
  BLOB((byte) 10),

  /* STRING */
  STRING((byte) 11),

  /* OBJECT */
  OBJECT((byte) 12);

  private final byte dataType;

  Type(byte type) {
    this.dataType = type;
  }

  public byte getType() {
    return dataType;
  }

  public static Type valueOf(byte type) {
    for (Type t : Type.values()) {
      if (t.dataType == type) {
        return t;
      }
    }
    throw new IllegalArgumentException("Unsupported type: " + type);
  }

  public boolean checkObjectType(Object o) {
    switch (this) {
      case BOOLEAN:
        return o instanceof Boolean;
      case INT32:
        return o instanceof Integer;
      case INT64:
      case TIMESTAMP:
        return o instanceof Long;
      case FLOAT:
        return o instanceof Float;
      case DOUBLE:
        return o instanceof Double;
      case DATE:
        return o instanceof LocalDate;
      case BLOB:
      case OBJECT:
        return o instanceof Binary;
      case STRING:
      case TEXT:
        return o instanceof String;
      default:
        return false;
    }
  }

  public static List<Type> allTypes() {
    return Arrays.asList(
        BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, TIMESTAMP, DATE, BLOB, STRING, OBJECT);
  }

  public static List<Type> numericTypes() {
    return Arrays.asList(INT32, INT64, FLOAT, DOUBLE);
  }
}
