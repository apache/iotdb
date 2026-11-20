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

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Select;

import java.util.List;
import java.util.Locale;

/**
 * The name PostgreSQL gives an output column without alias ({@code FigureColname}): a function's
 * name, a cast's operand name or else its type, {@code case}, {@code ?column?} for anything else.
 */
final class OutputNames {

  private OutputNames() {}

  /** The name, or {@code null} where the engine's name is already PostgreSQL's (columns, *). */
  static String of(Expression expression) {
    if (expression instanceof Column) {
      String name = Identifiers.fold(((Column) expression).getColumnName());
      // keywords that are values, not columns
      switch (name) {
        case "current_user":
        case "session_user":
        case "current_catalog":
        case "current_schema":
        case "user":
          return name;
        default:
          return null;
      }
    }
    if (expression instanceof AllColumns || expression instanceof Select) {
      return null;
    }
    String name = figure(expression);
    return name == null ? "?column?" : name;
  }

  private static String figure(Expression expression) {
    if (expression instanceof Column) {
      return Identifiers.fold(((Column) expression).getColumnName());
    }
    if (expression instanceof Function) {
      List<String> parts = ((Function) expression).getMultipartName();
      return Identifiers.fold(parts.get(parts.size() - 1));
    }
    if (expression instanceof CastExpression) {
      CastExpression cast = (CastExpression) expression;
      String inner = figure(cast.getLeftExpression());
      if (inner != null) {
        return inner;
      }
      String type = PgTypeNames.baseName(cast.getColDataType());
      return CANONICAL.getOrDefault(type, type);
    }
    if (expression instanceof CaseExpression) {
      return "case";
    }
    if (expression instanceof ExtractExpression) {
      return "extract";
    }
    if (expression instanceof TimeKeyExpression) {
      String key = ((TimeKeyExpression) expression).getStringValue().toLowerCase(Locale.ROOT);
      int paren = key.indexOf('(');
      return (paren < 0 ? key : key.substring(0, paren)).trim();
    }
    return null;
  }

  /** PostgreSQL's internal names of SQL type names, as a cast's column name shows them. */
  private static final java.util.Map<String, String> CANONICAL = new java.util.HashMap<>();

  static {
    CANONICAL.put("integer", "int4");
    CANONICAL.put("int", "int4");
    CANONICAL.put("smallint", "int2");
    CANONICAL.put("bigint", "int8");
    CANONICAL.put("real", "float4");
    CANONICAL.put("double precision", "float8");
    CANONICAL.put("float", "float8");
    CANONICAL.put("boolean", "bool");
    CANONICAL.put("character varying", "varchar");
    CANONICAL.put("timestamp with time zone", "timestamptz");
    CANONICAL.put("timestamp without time zone", "timestamp");
  }
}
