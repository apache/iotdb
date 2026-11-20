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

import org.apache.iotdb.postgresql.catalog.PgCatalogTables;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Queries over catalog tables that never have rows: a SELECT whose FROM reads such a table through
 * inner, cross or left joins (as the left side), without aggregates outside a GROUP BY, returns no
 * rows whatever it selects. Its select list becomes typed NULLs with the same output names, its
 * WHERE {@code FALSE}, so that constructs without a translation there (psql's {@code ANY(array)},
 * {@code ARRAY(...)}, statistics functions) do not refuse the statement. Runs before {@link
 * ConstructCheck}.
 */
final class EmptyRelations {

  private static final String PACKAGE = "net.sf.jsqlparser.";

  private static final Set<String> AGGREGATES =
      new HashSet<>(
          Arrays.asList(
              "count",
              "sum",
              "avg",
              "min",
              "max",
              "bool_and",
              "bool_or",
              "every",
              "string_agg",
              "array_agg",
              "stddev",
              "stddev_pop",
              "stddev_samp",
              "variance",
              "var_pop",
              "var_samp"));

  private final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
  private final List<PlainSelect> selects = new ArrayList<>();

  private EmptyRelations() {}

  /** Rewrites every qualifying SELECT of {@code statement} in place. */
  static void simplify(Object statement) {
    EmptyRelations finder = new EmptyRelations();
    finder.walk(statement);
    // innermost first: a SELECT over a derived table that is empty is empty as well
    java.util.Set<PlainSelect> emptied =
        java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    for (int i = finder.selects.size() - 1; i >= 0; i--) {
      PlainSelect select = finder.selects.get(i);
      String table = emptyTable(select);
      if (table != null) {
        empty(select, table);
        emptied.add(select);
      } else if (emptyDerived(select, emptied)) {
        emptyOverDerived(select);
        emptied.add(select);
      }
    }
  }

  /** The always-empty catalog table that makes {@code select} empty, or {@code null}. */
  private static String emptyTable(PlainSelect select) {
    if (select.getGroupBy() == null && hasAggregate(select.getSelectItems())) {
      // an aggregate over no rows still returns one row
      return null;
    }
    List<Join> joins = select.getJoins() == null ? new ArrayList<>() : select.getJoins();
    String first = catalogTable(select.getFromItem());
    if (first != null && PgCatalogTables.alwaysEmpty(first)) {
      for (Join join : joins) {
        if (join.isRight() || join.isFull()) {
          return null;
        }
      }
      return first;
    }
    for (Join join : joins) {
      String table = catalogTable(join.getFromItem());
      boolean inner = join.isInner() || join.isCross() || join.isSimple() || isPlain(join);
      if (table != null && PgCatalogTables.alwaysEmpty(table) && inner) {
        // an inner or cross join with an empty table is empty, wherever it stands
        return table;
      }
    }
    return null;
  }

  private static boolean isPlain(Join join) {
    return !join.isLeft()
        && !join.isRight()
        && !join.isFull()
        && !join.isOuter()
        && !join.isNatural()
        && !join.isSemi()
        && !join.isApply();
  }

  static boolean hasAggregate(List<SelectItem<?>> items) {
    for (SelectItem<?> item : items) {
      if (containsAggregate(item.getExpression(), new IdentityHashMap<>())) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsAggregate(Object node, IdentityHashMap<Object, Boolean> seen) {
    if (node == null || seen.containsKey(node)) {
      return false;
    }
    if (node instanceof Function
        && AGGREGATES.contains(((Function) node).getName().toLowerCase(Locale.ROOT))) {
      return true;
    }
    if (node instanceof Collection) {
      for (Object element : (Collection<?>) node) {
        if (containsAggregate(element, seen)) {
          return true;
        }
      }
      return false;
    }
    Class<?> type = node.getClass();
    if (!type.getName().startsWith(PACKAGE)
        || type.isEnum()
        || node instanceof net.sf.jsqlparser.statement.select.Select) {
      // a sub-query's aggregates are its own
      return false;
    }
    seen.put(node, Boolean.TRUE);
    for (Field field : fields(type)) {
      try {
        if (containsAggregate(field.get(node), seen)) {
          return true;
        }
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("cannot inspect " + field, e);
      }
    }
    return false;
  }

  /** True if {@code select} reads an emptied derived table (FROM or inner/cross join). */
  private static boolean emptyDerived(PlainSelect select, java.util.Set<PlainSelect> emptied) {
    if (select.getGroupBy() == null && hasAggregate(select.getSelectItems())) {
      return false;
    }
    if (isEmptied(select.getFromItem(), emptied)) {
      if (select.getJoins() != null) {
        for (Join join : select.getJoins()) {
          if (join.isRight() || join.isFull()) {
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }

  private static boolean isEmptied(FromItem item, java.util.Set<PlainSelect> emptied) {
    return item instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect
        && ((net.sf.jsqlparser.statement.select.ParenthesedSelect) item).getSelect()
            instanceof PlainSelect
        && emptied.contains(
            (PlainSelect)
                ((net.sf.jsqlparser.statement.select.ParenthesedSelect) item).getSelect());
  }

  /** Like {@link #empty}, keeping the (empty) derived table as the FROM item. */
  private static void emptyOverDerived(PlainSelect select) {
    List<SelectItem<?>> items = new ArrayList<>();
    for (SelectItem<?> item : select.getSelectItems()) {
      String name =
          item.getAlias() != null ? Identifiers.fold(item.getAlias().getName()) : outputName(item);
      items.add(typedNull(org.apache.tsfile.enums.TSDataType.STRING, name));
    }
    select.setSelectItems(items);
    select.setJoins(null);
    select.setWhere(new BooleanValue(false));
    select.setGroupByElement(null);
    select.setHaving(null);
    select.setDistinct(null);
    select.setOrderByElements(null);
    select.setLimit(null);
    select.setOffset(null);
    select.setFetch(null);
  }

  /** The catalog table a FROM item names (PgCatalogTables name), or {@code null}. */
  private static String catalogTable(FromItem item) {
    if (!(item instanceof Table)) {
      return null;
    }
    Table table = (Table) item;
    String schema = Identifiers.fold(table.getSchemaName());
    String name = Identifiers.fold(table.getName());
    String catalog = null;
    if (schema == null && name != null && name.startsWith("pg_")) {
      catalog = name;
    } else if ("pg_catalog".equals(schema)) {
      catalog = name;
    } else if ("information_schema".equals(schema)) {
      catalog = "pg_is_" + name;
    }
    return catalog != null && PgCatalogTables.contains(catalog) ? catalog : null;
  }

  private static void empty(PlainSelect select, String table) {
    List<SelectItem<?>> items = new ArrayList<>();
    for (SelectItem<?> item : select.getSelectItems()) {
      if (item.getExpression() instanceof net.sf.jsqlparser.statement.select.AllColumns) {
        for (PgCatalogTables.Column column : PgCatalogTables.tables().get(table)) {
          items.add(typedNull(column.type(), column.name()));
        }
        continue;
      }
      PgCatalogTables.Column column = column(item.getExpression(), table);
      String name =
          item.getAlias() != null ? Identifiers.fold(item.getAlias().getName()) : outputName(item);
      items.add(
          typedNull(
              column == null ? org.apache.tsfile.enums.TSDataType.STRING : column.type(), name));
    }
    select.setSelectItems(items);
    Table from = new Table("pg_catalog", PgCatalogTables.postgresName(table));
    if (table.startsWith(PgCatalogTables.INFORMATION_SCHEMA_PREFIX)) {
      from = new Table("information_schema", PgCatalogTables.postgresName(table));
    }
    select.setFromItem(from);
    select.setJoins(null);
    select.setWhere(new BooleanValue(false));
    select.setGroupByElement(null);
    select.setHaving(null);
    select.setDistinct(null);
    select.setOrderByElements(null);
    select.setLimit(null);
    select.setOffset(null);
    select.setFetch(null);
  }

  private static PgCatalogTables.Column column(Expression expression, String table) {
    if (expression instanceof Column) {
      return PgCatalogTables.column(table, Identifiers.fold(((Column) expression).getColumnName()));
    }
    return null;
  }

  /** The name PostgreSQL gives an output column without alias. */
  private static String outputName(SelectItem<?> item) {
    Expression expression = item.getExpression();
    while (expression instanceof CastExpression) {
      expression = ((CastExpression) expression).getLeftExpression();
    }
    if (expression instanceof Column) {
      return Identifiers.fold(((Column) expression).getColumnName());
    }
    if (expression instanceof Function) {
      List<String> parts = ((Function) expression).getMultipartName();
      return Identifiers.fold(parts.get(parts.size() - 1));
    }
    return "?column?";
  }

  private static SelectItem<?> typedNull(org.apache.tsfile.enums.TSDataType type, String name) {
    CastExpression cast = new CastExpression();
    cast.setLeftExpression(new NullValue());
    cast.setColDataType(new ColDataType(pgType(type)));
    SelectItem<CastExpression> item = new SelectItem<>(cast);
    item.setAlias(new Alias(Identifiers.quote(name), true));
    return item;
  }

  private static String pgType(org.apache.tsfile.enums.TSDataType type) {
    switch (type) {
      case INT32:
        return "int4";
      case INT64:
        return "int8";
      case FLOAT:
        return "float4";
      case DOUBLE:
        return "float8";
      case BOOLEAN:
        return "bool";
      default:
        return "text";
    }
  }

  // ---------------------------------------------------------------------------- walking

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
    if (!type.getName().startsWith(PACKAGE)
        || type.isEnum()
        || type.getName().startsWith(PACKAGE + "parser.")) {
      return;
    }
    visited.put(node, Boolean.TRUE);
    if (node instanceof PlainSelect) {
      selects.add((PlainSelect) node);
    }
    for (Field field : fields(type)) {
      try {
        walk(field.get(node));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("cannot inspect " + field, e);
      }
    }
  }

  private static List<Field> fields(Class<?> type) {
    List<Field> fields = new ArrayList<>();
    for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
          field.setAccessible(true);
          fields.add(field);
        }
      }
    }
    return fields;
  }
}
