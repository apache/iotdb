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

package org.apache.iotdb.postgresql.types;

import org.apache.tsfile.enums.TSDataType;

import java.util.Optional;

/**
 * The PostgreSQL types of table-model values, with their built-in OIDs, the OIDs of their array
 * types and their wire sizes.
 */
public enum PgType {
  BOOL(16, 1000, 1, "bool"),
  INT2(21, 1005, 2, "int2"),
  INT4(23, 1007, 4, "int4"),
  INT8(20, 1016, 8, "int8"),
  FLOAT4(700, 1021, 4, "float4"),
  FLOAT8(701, 1022, 8, "float8"),
  TEXT(25, 1009, -1, "text"),
  VARCHAR(1043, 1015, -1, "varchar"),
  BYTEA(17, 1001, -1, "bytea"),
  DATE(1082, 1182, 4, "date"),
  TIMESTAMP(1114, 1115, 8, "timestamp"),
  TIMESTAMPTZ(1184, 1185, 8, "timestamptz"),
  /** Parameters whose type the client leaves to the server. */
  UNKNOWN(705, 0, -2, "unknown");

  private final int oid;
  private final int arrayOid;
  private final short size;
  private final String typeName;

  PgType(int oid, int arrayOid, int size, String typeName) {
    this.oid = oid;
    this.arrayOid = arrayOid;
    this.size = (short) size;
    this.typeName = typeName;
  }

  public int oid() {
    return oid;
  }

  public int arrayOid() {
    return arrayOid;
  }

  /** {@code typlen}: bytes of a value, -1 for variable length, -2 for C strings. */
  public short size() {
    return size;
  }

  public String typeName() {
    return typeName;
  }

  /**
   * The PostgreSQL type of a table-model column; empty for types that are not exposed (OBJECT,
   * VECTOR, UNKNOWN).
   */
  public static Optional<PgType> of(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Optional.of(BOOL);
      case INT32:
        return Optional.of(INT4);
      case INT64:
        return Optional.of(INT8);
      case FLOAT:
        return Optional.of(FLOAT4);
      case DOUBLE:
        return Optional.of(FLOAT8);
      case TEXT:
      case STRING:
        return Optional.of(TEXT);
      case BLOB:
        return Optional.of(BYTEA);
      case DATE:
        return Optional.of(DATE);
      case TIMESTAMP:
        return Optional.of(TIMESTAMPTZ);
      default:
        return Optional.empty();
    }
  }

  /** The type with this OID, or empty if it is not one the adapter knows. */
  public static Optional<PgType> ofOid(int oid) {
    for (PgType type : values()) {
      if (type.oid == oid) {
        return Optional.of(type);
      }
    }
    return Optional.empty();
  }
}
