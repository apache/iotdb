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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.StringValueFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AbstractSingleChildFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.NotFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema.CheckSchemaPredicateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema.ConvertSchemaPredicateToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaPredicateUtil {

  private SchemaPredicateUtil() {}

  // pair.left is Expressions only contain ID columns
  // pair.right is Expressions contain at least one ATTRIBUTE column
  static Pair<List<Expression>, List<Expression>> separateIdDeterminedPredicate(
      final List<Expression> expressionList,
      final TsTable table,
      final MPPQueryContext queryContext) {
    final List<Expression> idDeterminedList = new ArrayList<>();
    final List<Expression> idFuzzyList = new ArrayList<>();
    final CheckSchemaPredicateVisitor visitor = new CheckSchemaPredicateVisitor();
    final CheckSchemaPredicateVisitor.Context context =
        new CheckSchemaPredicateVisitor.Context(table, queryContext);
    for (final Expression expression : expressionList) {
      if (expression == null) {
        continue;
      }
      if (Boolean.TRUE.equals(expression.accept(visitor, context))) {
        idFuzzyList.add(expression);
      } else {
        idDeterminedList.add(expression);
      }
    }
    return new Pair<>(idDeterminedList, idFuzzyList);
  }

  // input and-concat filter list
  // return or concat filter list, inner which all filter is and concat
  // e.g. (a OR b) AND (c OR d) -> (a AND c) OR (a AND d) OR (b AND c) OR (b AND d)
  // if input is empty, then return [[]]
  static List<Map<Integer, List<SchemaFilter>>> convertDeviceIdPredicateToOrConcatList(
      final List<Expression> schemaFilterList, final TsTable table) {
    final ConvertSchemaPredicateToFilterVisitor visitor =
        new ConvertSchemaPredicateToFilterVisitor();
    final ConvertSchemaPredicateToFilterVisitor.Context context =
        new ConvertSchemaPredicateToFilterVisitor.Context(table);

    final List<List<Expression>> orConcatList =
        schemaFilterList.stream()
            .map(IrUtils::extractOrPredicatesWithInExpanded)
            .collect(Collectors.toList());
    final int orSize = orConcatList.size();
    int remainingCaseNum = 1;
    for (final List<Expression> filterList : orConcatList) {
      remainingCaseNum *= filterList.size();
    }
    final List<Map<Integer, List<SchemaFilter>>> result = new ArrayList<>();
    final int[] indexes = new int[orSize]; // index count, each case represents one possible result
    boolean hasConflictFilter;
    Expression currentExpression;
    while (remainingCaseNum > 0) {
      hasConflictFilter = false;
      final Map<Integer, List<SchemaFilter>> oneCase = new HashMap<>();
      for (int j = 0; j < orSize; j++) {
        currentExpression = orConcatList.get(j).get(indexes[j]);
        if (!handleFilter(
            (AbstractSingleChildFilter) currentExpression.accept(visitor, context), oneCase)) {
          hasConflictFilter = true;
          break;
        }
      }

      if (!hasConflictFilter) {
        result.add(oneCase);
      }

      for (int k = orSize - 1; k >= 0; k--) {
        indexes[k]++;
        if (indexes[k] < orConcatList.get(k).size()) {
          break;
        }
        indexes[k] = 0;
      }
      remainingCaseNum--;
    }
    return result;
  }

  private static boolean handleFilter(
      final AbstractSingleChildFilter filter,
      final Map<Integer, List<SchemaFilter>> index2FilterMap) {
    // We assume that only "not" and "IdFilter" is possible here to be the root filter
    // There won't be any attribute filters here currently

    // First refactor the not filters
    AbstractSingleChildFilter currentFilter = filter;
    boolean isNotFilter = false;
    while (currentFilter.getSchemaFilterType().equals(SchemaFilterType.NOT)) {
      currentFilter = (AbstractSingleChildFilter) currentFilter.getChild();
      isNotFilter = !isNotFilter;
    }

    final int index = ((IdFilter) currentFilter).getIndex();
    final SchemaFilter childFilter = currentFilter.getChild();

    // Compress the not filters and put them after idFilter,
    // to simplify the logics in schema regions
    if (isNotFilter) {
      currentFilter.setChild(new NotFilter(childFilter));
    }

    if (!index2FilterMap.containsKey(index)) {
      index2FilterMap.computeIfAbsent(index, k -> new ArrayList<>()).add(currentFilter);
    } else if (!isNotFilter && childFilter.getSchemaFilterType().equals(SchemaFilterType.PRECISE)) {
      if (index2FilterMap.get(index).stream()
          .allMatch(
              existingFilter ->
                  Boolean.TRUE.equals(
                      existingFilter.accept(
                          StringValueFilterVisitor.getInstance(),
                          ((PreciseFilter) childFilter).getValue())))) {
        index2FilterMap.put(index, Collections.singletonList(currentFilter));
      } else {
        return false;
      }
    } else {
      final SchemaFilter firstFilter = index2FilterMap.get(index).get(0);
      if ((firstFilter.getSchemaFilterType().equals(SchemaFilterType.ID))
          && ((IdFilter) firstFilter)
              .getChild()
              .getSchemaFilterType()
              .equals(SchemaFilterType.PRECISE)) {
        return Boolean.TRUE.equals(
            currentFilter.accept(
                StringValueFilterVisitor.getInstance(),
                ((PreciseFilter) ((IdFilter) firstFilter).getChild()).getValue()));
      } else {
        index2FilterMap.get(index).add(currentFilter);
      }
    }
    return true;
  }

  public static String getColumnName(Expression expression) {
    ComparisonExpression node = (ComparisonExpression) expression;
    String columnName;
    if (node.getLeft() instanceof Literal) {
      if (!(node.getRight() instanceof SymbolReference)) {
        throw new IllegalStateException("Can only be SymbolReference, now is " + node.getRight());
      }
      columnName = ((SymbolReference) (node.getRight())).getName();
    } else {
      if (!(node.getLeft() instanceof SymbolReference)) {
        throw new IllegalStateException("Can only be SymbolReference, now is " + node.getLeft());
      }
      columnName = ((SymbolReference) (node.getLeft())).getName();
    }
    return columnName;
  }

  static List<Integer> extractIdSingleMatchExpressionCases(
      final List<Map<Integer, List<SchemaFilter>>> index2FilterMapList,
      final TsTable tableInstance) {
    final List<Integer> selectedExpressionCases = new ArrayList<>();
    final int idCount = tableInstance.getIdNums();
    for (int i = 0; i < index2FilterMapList.size(); i++) {
      final Map<Integer, List<SchemaFilter>> filterMap = index2FilterMapList.get(i);
      if (filterMap.size() == idCount
          && filterMap.values().stream()
              .allMatch(
                  filterList ->
                      filterList.get(0).getSchemaFilterType().equals(SchemaFilterType.ID)
                          && ((IdFilter) filterList.get(0))
                              .getChild()
                              .getSchemaFilterType()
                              .equals(SchemaFilterType.PRECISE))) {
        selectedExpressionCases.add(i);
      }
    }
    return selectedExpressionCases;
  }

  // compact and-concat expression list to one expression
  static Expression compactDeviceIdFuzzyPredicate(final List<Expression> expressionList) {
    if (expressionList.isEmpty()) {
      return null;
    }
    return expressionList.size() > 1
        ? new LogicalExpression(LogicalExpression.Operator.AND, expressionList)
        : expressionList.get(0);
  }
}
