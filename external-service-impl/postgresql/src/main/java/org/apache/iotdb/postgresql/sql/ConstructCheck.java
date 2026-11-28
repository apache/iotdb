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

import org.apache.iotdb.postgresql.i18n.PostgreSqlMessages;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.WithItem;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Checks a parsed statement against the constructs with an exact translation and prepares it: every
 * node class must be on the allow list, and flags that change the meaning of an allowed node
 * (DISTINCT ON, NATURAL JOIN, ORDER BY inside an aggregate, ...) are refused. Walks the syntax tree
 * through its fields, so a construct the list does not know can never pass unnoticed.
 *
 * <p>Preparation: ORDER BY items get PostgreSQL's default null ordering (NULLS LAST ascending,
 * NULLS FIRST descending) written out, and the names of common table expressions are collected.
 */
final class ConstructCheck {

  private static final String PACKAGE = "net.sf.jsqlparser.";

  private static final Set<String> ALLOWED =
      new HashSet<>(
          Arrays.asList(
              "statement.select.PlainSelect",
              "statement.select.SetOperationList",
              "statement.select.UnionOp",
              "statement.select.ParenthesedSelect",
              "statement.select.SelectItem",
              "statement.select.AllColumns",
              "statement.select.AllTableColumns",
              "statement.select.Join",
              "statement.select.OrderByElement",
              "statement.select.GroupByElement",
              "statement.select.Limit",
              "statement.select.Offset",
              "statement.select.Fetch",
              "statement.select.Distinct",
              "statement.select.WithItem",
              "statement.select.ParenthesedFromItem",
              "statement.create.table.ColDataType",
              "schema.Table",
              "schema.Column",
              "schema.Database",
              "schema.Server",
              "expression.Alias",
              "expression.BooleanValue",
              "expression.LongValue",
              "expression.DoubleValue",
              "expression.StringValue",
              "expression.NullValue",
              "expression.JdbcParameter",
              "expression.Function",
              "expression.CastExpression",
              "expression.CaseExpression",
              "expression.WhenClause",
              "expression.SignedExpression",
              "expression.NotExpression",
              "expression.TimeKeyExpression",
              "expression.operators.conditional.AndExpression",
              "expression.operators.conditional.OrExpression",
              "expression.operators.relational.EqualsTo",
              "expression.operators.relational.NotEqualsTo",
              "expression.operators.relational.GreaterThan",
              "expression.operators.relational.GreaterThanEquals",
              "expression.operators.relational.MinorThan",
              "expression.operators.relational.MinorThanEquals",
              "expression.operators.relational.InExpression",
              "expression.operators.relational.Between",
              "expression.operators.relational.IsNullExpression",
              "expression.operators.relational.IsBooleanExpression",
              "expression.operators.relational.LikeExpression",
              "expression.operators.relational.RegExpMatchOperator",
              "expression.ExtractExpression",
              "expression.IntervalExpression",
              "expression.IntervalQualifier",
              "expression.DateTimeLiteralExpression",
              "expression.DateUnitExpression",
              "expression.ArrayExpression",
              "expression.AnalyticExpression",
              "expression.WindowDefinition",
              "expression.PartitionByClause",
              "expression.OrderByClause",
              "expression.operators.relational.ExistsExpression",
              "expression.operators.relational.ExpressionList",
              "expression.operators.relational.ParenthesedExpressionList",
              "expression.operators.arithmetic.Addition",
              "expression.operators.arithmetic.Subtraction",
              "expression.operators.arithmetic.Multiplication",
              "expression.operators.arithmetic.Division",
              "expression.operators.arithmetic.Modulo",
              "expression.operators.arithmetic.Concat"));

  private static final Map<String, String> NAMES = new HashMap<>();

  static {
    NAMES.put("ArrayConstructor", "ARRAY");
    NAMES.put("AnyComparisonExpression", "ANY/ALL/SOME");
    NAMES.put("IntervalExpression", "INTERVAL");
    NAMES.put("Values", "VALUES");
    NAMES.put("ParenthesedValues", "VALUES");
    NAMES.put("ExtractExpression", "EXTRACT");
    NAMES.put("IntersectOp", "INTERSECT");
    NAMES.put("ExceptOp", "EXCEPT");
    NAMES.put("MinusOp", "MINUS");
    NAMES.put("SimilarToExpression", "SIMILAR TO");
    NAMES.put("RegExpMatchOperator", "regular expression operators");
    NAMES.put("JsonExpression", "JSON operators");
    NAMES.put("JsonOperator", "JSON operators");
    NAMES.put("ForClause", "FOR UPDATE/SHARE");
    NAMES.put("LateralSubSelect", "LATERAL");
    NAMES.put("TableFunction", "table functions in FROM");
    NAMES.put("JdbcNamedParameter", "named parameters");
    NAMES.put("UserVariable", "variables");
    NAMES.put("HexValue", "hexadecimal literals");
    NAMES.put("DateValue", "{d ...} literals");
    NAMES.put("TimestampValue", "{ts ...} literals");
    NAMES.put("TimeValue", "{t ...} literals");
    NAMES.put("CollateExpression", "COLLATE");
    NAMES.put("TrimFunction", "TRIM");
    NAMES.put("OverlapsCondition", "OVERLAPS");
    NAMES.put("RowConstructor", "row constructors");
    NAMES.put("XorExpression", "XOR");
    NAMES.put("BitwiseAnd", "bitwise operators");
    NAMES.put("BitwiseOr", "bitwise operators");
    NAMES.put("BitwiseXor", "bitwise operators");
    NAMES.put("BitwiseLeftShift", "bitwise operators");
    NAMES.put("BitwiseRightShift", "bitwise operators");
    NAMES.put("IntegerDivision", "DIV");
    NAMES.put("GroupingSets", "GROUPING SETS");
  }

  private static final Map<Class<?>, Field[]> FIELDS = new ConcurrentHashMap<>();

  private final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
  private final Set<String> cteNames = new HashSet<>();

  /** Table qualifier (alias or name) → catalog table it refers to; {@code null} if ambiguous. */
  private final Map<String, String> catalogTables = new HashMap<>();

  private boolean otherTables;

  /** What the check found out about the statement. */
  static final class Result {
    final Set<String> cteNames;
    final Map<String, String> catalogTables;
    final boolean otherTables;

    Result(Set<String> cteNames, Map<String, String> catalogTables, boolean otherTables) {
      this.cteNames = cteNames;
      this.catalogTables = catalogTables;
      this.otherTables = otherTables;
    }
  }

  private ConstructCheck() {}

  /**
   * Checks and prepares the statement.
   *
   * @return the folded names of its common table expressions
   * @throws UnsupportedConstructException naming the first construct without exact translation
   */
  static Result check(Object statement) {
    ConstructCheck check = new ConstructCheck();
    check.walk(statement);
    // a CTE named like a catalog table hides it
    check.catalogTables.keySet().removeIf(check.cteNames::contains);
    return new Result(check.cteNames, check.catalogTables, check.otherTables);
  }

  /** Records which catalog table a table reference names, under its alias or name. */
  private void recordTable(net.sf.jsqlparser.schema.Table table) {
    String schema = Identifiers.fold(table.getSchemaName());
    String name = Identifiers.fold(table.getName());
    if (name == null) {
      return;
    }
    String catalog = null;
    if (schema == null && name.startsWith("pg_")) {
      catalog = name;
    } else if ("pg_catalog".equals(schema)) {
      catalog = name;
    } else if ("information_schema".equals(schema)) {
      catalog = "pg_is_" + name;
    }
    String key = table.getAlias() != null ? Identifiers.fold(table.getAlias().getName()) : name;
    if (catalog == null || !org.apache.iotdb.postgresql.catalog.PgCatalogTables.contains(catalog)) {
      // a user table (or a CTE) under the same qualifier makes the qualifier ambiguous
      otherTables = true;
      catalogTables.put(key, null);
      return;
    }
    if (catalogTables.containsKey(key) && !catalog.equals(catalogTables.get(key))) {
      catalogTables.put(key, null);
    } else {
      catalogTables.put(key, catalog);
    }
  }

  private void walk(Object node) {
    if (node == null || visited.containsKey(node)) {
      return;
    }
    if (node instanceof Collection) {
      for (Object element : (Collection<?>) node) {
        walk(element);
      }
      return;
    }
    Class<?> type = node.getClass();
    if (type.isArray()) {
      if (!type.getComponentType().isPrimitive()) {
        for (int i = 0; i < Array.getLength(node); i++) {
          walk(Array.get(node, i));
        }
      }
      return;
    }
    String name = type.getName();
    if (!name.startsWith(PACKAGE) || type.isEnum() || name.startsWith(PACKAGE + "parser.")) {
      return;
    }
    visited.put(node, Boolean.TRUE);
    String relative = name.substring(PACKAGE.length());
    if (!ALLOWED.contains(relative)) {
      throw new UnsupportedConstructException(describe(type));
    }
    checkFlags(node);
    for (Field field : fields(type)) {
      try {
        walk(field.get(node));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(
            String.format(PostgreSqlMessages.EXCEPTION_CANNOT_INSPECT_ARG_A8699317, field), e);
      }
    }
  }

  private void checkFlags(Object node) {
    if (node instanceof net.sf.jsqlparser.schema.Column) {
      // the qualifier of a column is a Table node too, but not a table reference
      net.sf.jsqlparser.schema.Table qualifier =
          ((net.sf.jsqlparser.schema.Column) node).getTable();
      if (qualifier != null) {
        visited.put(qualifier, Boolean.TRUE);
      }
    } else if (node instanceof net.sf.jsqlparser.schema.Table) {
      recordTable((net.sf.jsqlparser.schema.Table) node);
    } else if (node instanceof net.sf.jsqlparser.expression.AnalyticExpression) {
      checkWindow((net.sf.jsqlparser.expression.AnalyticExpression) node);
    } else if (node instanceof PlainSelect) {
      PlainSelect select = (PlainSelect) node;
      if (select.getIntoTables() != null || select.getIntoTempTable() != null) {
        throw new UnsupportedConstructException("SELECT INTO");
      }
      if (select.getWindowDefinitions() != null && !select.getWindowDefinitions().isEmpty()) {
        throw new UnsupportedConstructException("WINDOW clauses");
      }
      if (select.getForMode() != null) {
        throw new UnsupportedConstructException("FOR UPDATE/SHARE");
      }
    } else if (node instanceof Distinct) {
      Distinct distinct = (Distinct) node;
      if (distinct.getOnSelectItems() != null && !distinct.getOnSelectItems().isEmpty()) {
        throw new UnsupportedConstructException("DISTINCT ON");
      }
      if (distinct.isUseUnique() || distinct.isUseDistinctRow()) {
        throw new UnsupportedConstructException("SELECT UNIQUE");
      }
    } else if (node instanceof Join) {
      Join join = (Join) node;
      if (join.isNatural()) {
        throw new UnsupportedConstructException("NATURAL JOIN");
      }
      if (join.isApply() || join.isSemi() || join.isStraight() || join.isWindowJoin()) {
        throw new UnsupportedConstructException("this kind of JOIN");
      }
    } else if (node instanceof Function) {
      Function function = (Function) node;
      if (function.getOrderByElements() != null && !function.getOrderByElements().isEmpty()) {
        throw new UnsupportedConstructException("ORDER BY inside " + function.getName() + "()");
      }
      // Function.getAttribute()/getAttributeName() fail on functions without one (JSqlParser 5.4)
      if (field(function, "attributeExpression") != null
          || field(function, "attributeColumn") != null) {
        throw new UnsupportedConstructException("field access on " + function.getName() + "()");
      }
      if (function.isUnique()
          || function.isIgnoreNulls()
          || function.getNullHandling() != null
          || function.getExtraKeyword() != null
          || (function.getKeywordArguments() != null && !function.getKeywordArguments().isEmpty())
          || function.getKeep() != null
          || function.getLimit() != null
          || function.getHavingClause() != null
          || function.getNamedParameters() != null
          || function.getChainedParameters() != null) {
        throw new UnsupportedConstructException("this form of " + function.getName() + "()");
      }
    } else if (node instanceof OrderByElement) {
      OrderByElement order = (OrderByElement) node;
      if (order.getNullOrdering() == null) {
        // PostgreSQL's defaults, written out so the engine's defaults do not matter
        order.setNullOrdering(
            order.isAsc()
                ? OrderByElement.NullOrdering.NULLS_LAST
                : OrderByElement.NullOrdering.NULLS_FIRST);
      }
    } else if (node instanceof WithItem) {
      WithItem<?> with = (WithItem<?>) node;
      if (with.isRecursive()) {
        throw new UnsupportedConstructException("WITH RECURSIVE");
      }
      if (with.getWithItemList() != null && !with.getWithItemList().isEmpty()) {
        throw new UnsupportedConstructException("column lists of WITH queries");
      }
      if (with.getAlias() != null) {
        cteNames.add(Identifiers.fold(with.getAlias().getName()));
      }
    }
  }

  /** Window functions the engine has with PostgreSQL's meaning, and aggregates over windows. */
  private static final Set<String> WINDOW_FUNCTIONS =
      new HashSet<>(
          Arrays.asList(
              "row_number",
              "rank",
              "dense_rank",
              "percent_rank",
              "cume_dist",
              "ntile",
              "first_value",
              "last_value",
              "nth_value",
              "lead",
              "lag",
              "count",
              "sum",
              "avg",
              "min",
              "max"));

  private static void checkWindow(net.sf.jsqlparser.expression.AnalyticExpression window) {
    String name = window.getName().toLowerCase(java.util.Locale.ROOT);
    if (!WINDOW_FUNCTIONS.contains(name)) {
      throw new UnsupportedConstructException("the window function " + name + "()");
    }
    if (window.getType() != net.sf.jsqlparser.expression.AnalyticType.OVER
        || window.getWindowElement() != null
        || window.getWindowName() != null
        || window.isDistinct()
        || window.isUnique()
        || window.isIgnoreNulls()
        || window.isIgnoreNullsOutside()
        || window.getFilterExpression() != null
        || window.getKeep() != null
        || window.getFuncOrderBy() != null) {
      // explicit frames, FILTER, named windows, DISTINCT and IGNORE NULLS stay out
      throw new UnsupportedConstructException("this form of the window function " + name + "()");
    }
  }

  private static Object field(Object node, String name) {
    for (Class<?> c = node.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
      try {
        Field field = c.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(node);
      } catch (NoSuchFieldException e) {
        // look in the superclass
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(
            String.format(PostgreSqlMessages.EXCEPTION_CANNOT_INSPECT_ARG_A8699317, name), e);
      }
    }
    return null;
  }

  private static Field[] fields(Class<?> type) {
    return FIELDS.computeIfAbsent(
        type,
        t -> {
          java.util.List<Field> result = new java.util.ArrayList<>();
          for (Class<?> c = t; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
              if (Modifier.isStatic(field.getModifiers())
                  || field.getType().isPrimitive()
                  || field.getType() == String.class) {
                continue;
              }
              field.setAccessible(true);
              result.add(field);
            }
          }
          return result.toArray(new Field[0]);
        });
  }

  private static String describe(Class<?> type) {
    String simple = type.getSimpleName();
    String known = NAMES.get(simple);
    if (known != null) {
      return known;
    }
    // CamelCase class name to words: "SimilarToExpression" -> "similar to expression"
    return simple.replaceAll("([a-z])([A-Z])", "$1 $2").toLowerCase(java.util.Locale.ROOT);
  }
}
