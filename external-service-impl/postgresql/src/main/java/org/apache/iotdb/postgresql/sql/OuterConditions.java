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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * The conditions of a sub-query's WHERE that only read the outer row: every column they use is
 * qualified with a name the sub-query does not define. Used to move them in front of the sub-query,
 * where the engine can evaluate them.
 */
final class OuterConditions {

  private OuterConditions() {}

  /**
   * The outer-only conjuncts of {@code select}'s WHERE; empty if there are none or the sub-query
   * aggregates or groups (then no rows does not mean NULL).
   */
  static List<Expression> split(PlainSelect select) {
    if (select.getWhere() == null
        || select.getGroupBy() != null
        || select.getHaving() != null
        || EmptyRelations.hasAggregate(select.getSelectItems())) {
      return Collections.emptyList();
    }
    Set<String> local = new HashSet<>();
    addName(local, select.getFromItem());
    if (select.getJoins() != null) {
      for (Join join : select.getJoins()) {
        addName(local, join.getFromItem());
      }
    }
    List<Expression> outer = new ArrayList<>();
    for (Expression conjunct : conjuncts(select.getWhere())) {
      List<Column> columns = new ArrayList<>();
      if (!columns(conjunct, columns, new IdentityHashMap<>()) || columns.isEmpty()) {
        continue;
      }
      boolean onlyOuter = true;
      for (Column column : columns) {
        Table table = column.getTable();
        if (table == null
            || table.getName() == null
            || table.getSchemaName() != null
            || local.contains(Identifiers.fold(table.getName()))) {
          onlyOuter = false;
          break;
        }
      }
      if (onlyOuter) {
        outer.add(conjunct);
      }
    }
    if (outer.size() == conjuncts(select.getWhere()).size()) {
      // nothing would stay inside: keep the statement as it is
      return Collections.emptyList();
    }
    return outer;
  }

  /** The WHERE without {@code removed} (by identity); {@code null} if nothing remains. */
  static Expression remaining(Expression where, List<Expression> removed) {
    List<Expression> kept = new ArrayList<>();
    for (Expression conjunct : conjuncts(where)) {
      boolean drop = false;
      for (Expression r : removed) {
        drop |= r == conjunct;
      }
      if (!drop) {
        kept.add(conjunct);
      }
    }
    return kept.isEmpty() ? null : and(kept);
  }

  static Expression and(List<Expression> conjuncts) {
    Expression result = conjuncts.get(0);
    for (int i = 1; i < conjuncts.size(); i++) {
      result = new AndExpression(result, conjuncts.get(i));
    }
    return result;
  }

  private static List<Expression> conjuncts(Expression where) {
    List<Expression> result = new ArrayList<>();
    if (where instanceof AndExpression) {
      result.addAll(conjuncts(((AndExpression) where).getLeftExpression()));
      result.addAll(conjuncts(((AndExpression) where).getRightExpression()));
    } else {
      result.add(where);
    }
    return result;
  }

  private static void addName(Set<String> names, FromItem item) {
    if (item == null) {
      return;
    }
    if (item.getAlias() != null) {
      names.add(Identifiers.fold(item.getAlias().getName()));
    }
    if (item instanceof Table) {
      names.add(Identifiers.fold(((Table) item).getName()));
    }
  }

  /** Collects the columns of {@code node}; false if it contains a sub-query. */
  private static boolean columns(
      Object node, List<Column> columns, IdentityHashMap<Object, Boolean> seen) {
    if (node == null || seen.containsKey(node)) {
      return true;
    }
    if (node instanceof Select) {
      return false;
    }
    if (node instanceof Column) {
      columns.add((Column) node);
      return true;
    }
    if (node instanceof Collection) {
      for (Object element : (Collection<?>) node) {
        if (!columns(element, columns, seen)) {
          return false;
        }
      }
      return true;
    }
    Class<?> type = node.getClass();
    if (!type.getName().startsWith("net.sf.jsqlparser.") || type.isEnum()) {
      return true;
    }
    seen.put(node, Boolean.TRUE);
    for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
          continue;
        }
        field.setAccessible(true);
        try {
          if (!columns(field.get(node), columns, seen)) {
            return false;
          }
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(
              String.format(PostgreSqlMessages.EXCEPTION_CANNOT_INSPECT_ARG_A8699317, field), e);
        }
      }
    }
    return true;
  }
}
