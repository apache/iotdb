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

package org.apache.iotdb.postgresql.sql;

import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** PostgreSQL type names in casts, mapped to table-model types. */
final class PgTypeNames {

  private static final Map<String, String> TYPES = new HashMap<>();

  static {
    for (String name : new String[] {"bool", "boolean"}) {
      TYPES.put(name, "BOOLEAN");
    }
    for (String name : new String[] {"int2", "smallint", "int4", "int", "integer"}) {
      TYPES.put(name, "INT32");
    }
    for (String name : new String[] {"int8", "bigint", "oid"}) {
      TYPES.put(name, "INT64");
    }
    for (String name : new String[] {"float4", "real"}) {
      TYPES.put(name, "FLOAT");
    }
    for (String name : new String[] {"float8", "double precision", "float"}) {
      TYPES.put(name, "DOUBLE");
    }
    for (String name :
        new String[] {
          "text", "varchar", "character varying", "char", "character", "bpchar", "name"
        }) {
      TYPES.put(name, "STRING");
    }
    for (String name :
        new String[] {
          "timestamptz", "timestamp", "timestamp with time zone", "timestamp without time zone"
        }) {
      TYPES.put(name, "TIMESTAMP");
    }
    TYPES.put("date", "DATE");
    TYPES.put("bytea", "BLOB");
  }

  private PgTypeNames() {}

  /** The name of a cast target: lower case, without {@code pg_catalog.} and modifiers. */
  static String baseName(ColDataType type) {
    String name = type.getDataType().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    if (name.startsWith("pg_catalog.")) {
      name = name.substring("pg_catalog.".length());
    }
    int open = name.indexOf('(');
    if (open >= 0) {
      name = name.substring(0, open).trim();
    }
    return Identifiers.fold(name);
  }

  /** The table-model type for a cast target. */
  static String tableType(ColDataType type) {
    String name = type.getDataType().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    if (name.startsWith("pg_catalog.")) {
      name = name.substring("pg_catalog.".length());
    }
    boolean modifiers = false;
    int open = name.indexOf('(');
    if (open >= 0) {
      // JSqlParser keeps the type modifiers in the name: "varchar (10)", "numeric (10, 2)"
      modifiers = true;
      name = name.substring(0, open).trim();
    }
    name = Identifiers.fold(name);
    if (type.getArrayData() != null && !type.getArrayData().isEmpty()) {
      throw new UnsupportedConstructException("casts to array types");
    }
    String mapped = TYPES.get(name);
    if (mapped == null) {
      throw new UnsupportedConstructException("casts to " + name);
    }
    List<String> arguments = type.getArgumentsStringList();
    boolean hasArguments = modifiers || (arguments != null && !arguments.isEmpty());
    if (hasArguments && !mapped.equals("STRING") && !name.startsWith("timestamp")) {
      throw new UnsupportedConstructException("casts to " + name + " with modifiers");
    }
    return mapped;
  }
}
