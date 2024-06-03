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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SymbolReference;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators;
import org.apache.tsfile.utils.Binary;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor.getLongValue;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor.isTimeColumn;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

public class ConvertPredicateToFilterVisitor
    extends PredicateVisitor<Filter, ConvertPredicateToFilterVisitor.Context> {

  private static final ConvertPredicateToTimeFilterVisitor TIME_FILTER_VISITOR =
      new ConvertPredicateToTimeFilterVisitor();

  @Override
  protected Filter visitInPredicate(InPredicate node, Context context) {
    Expression operand = node.getValue();
    if (isTimeColumn(operand)) {
      return TIME_FILTER_VISITOR.process(node, null);
    }

    checkArgument(isSymbolReference(operand));

    Expression valueList = node.getValueList();
    checkArgument(valueList instanceof InListExpression);
    List<Expression> values = ((InListExpression) valueList).getValues();
    for (Expression value : values) {
      checkArgument(value instanceof Literal);
    }

    if (values.size() == 1) {
      return constructCompareFilter(
          ComparisonExpression.Operator.EQUAL,
          (SymbolReference) operand,
          (Literal) values.get(0),
          context);
    }

    return constructInFilter(
        (SymbolReference) operand,
        values.stream().map(v -> (Literal) v).collect(Collectors.toList()),
        context);
  }

  private <T extends Comparable<T>> ValueFilterOperators.ValueIn<T> constructInFilter(
      SymbolReference operand, List<Literal> values, Context context) {
    int measurementIndex = context.getMeasurementIndex((operand).getName());
    Set<T> inSet = constructInSet(values, context.getType(Symbol.from(operand)));
    return ValueFilterApi.in(measurementIndex, inSet);
  }

  private <T extends Comparable<T>> Set<T> constructInSet(List<Literal> literals, Type dataType) {
    Set<T> values = new HashSet<>();
    for (Literal literal : literals) {
      values.add(getValue(literal, dataType));
    }
    return values;
  }

  public static <T extends Comparable<T>> Filter constructCompareFilter(
      ComparisonExpression.Operator operator,
      SymbolReference symbolReference,
      Literal literal,
      Context context) {

    if (!context.isMeasurementColumn(symbolReference)) {
      throw new IllegalStateException(
          String.format("Only support measurement column in filter: %s", symbolReference));
    }

    int measurementIndex = context.getMeasurementIndex(symbolReference.getName());
    Type type = context.getType(Symbol.from(symbolReference));

    T value = getValue(literal, type);

    switch (operator) {
      case EQUAL:
        return ValueFilterApi.eq(measurementIndex, value);
      case NOT_EQUAL:
        return ValueFilterApi.notEq(measurementIndex, value);
      case GREATER_THAN:
        return ValueFilterApi.gt(measurementIndex, value);
      case GREATER_THAN_OR_EQUAL:
        return ValueFilterApi.gtEq(measurementIndex, value);
      case LESS_THAN:
        return ValueFilterApi.lt(measurementIndex, value);
      case LESS_THAN_OR_EQUAL:
        return ValueFilterApi.ltEq(measurementIndex, value);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported comparison operator %s", operator));
    }
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> T getValue(Literal value, Type dataType) {
    try {
      switch (dataType.getTypeEnum()) {
        case INT32:
          return (T) Integer.valueOf(Long.valueOf(getLongValue(value)).intValue());
        case INT64:
          return (T) Long.valueOf(getLongValue(value));
        case FLOAT:
          return (T) Float.valueOf((float) getDoubleValue(value));
        case DOUBLE:
          return (T) Double.valueOf(getDoubleValue(value));
        case BOOLEAN:
          return (T) Boolean.valueOf(getBooleanValue(value));
        case TEXT:
          return (T) new Binary(getStringValue(value), TSFileConfig.STRING_CHARSET);
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported data type %s", dataType));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("\"%s\" cannot be cast to [%s]", value, dataType));
    }
  }

  @Override
  protected Filter visitIsNullPredicate(IsNullPredicate node, Context context) {
    throw new IllegalArgumentException("IS NULL cannot be pushed down");
  }

  @Override
  protected Filter visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
    checkArgument(isSymbolReference(node.getValue()));
    SymbolReference operand = (SymbolReference) node.getValue();
    checkArgument(context.isMeasurementColumn(operand));
    int measurementIndex = context.getMeasurementIndex(operand.getName());
    return ValueFilterApi.isNotNull(measurementIndex);
  }

  @Override
  protected Filter visitLikePredicate(LikePredicate node, Context context) {
    checkArgument(isSymbolReference(node.getValue()));
    SymbolReference operand = (SymbolReference) node.getValue();
    checkArgument(context.isMeasurementColumn(operand));
    int measurementIndex = context.getMeasurementIndex(operand.getName());
    Expression pattern = node.getPattern();
    return ValueFilterApi.like(measurementIndex, getStringValue(pattern));
  }

  @Override
  protected Filter visitLogicalExpression(LogicalExpression node, Context context) {
    switch (node.getOperator()) {
      case OR:
        return FilterFactory.or(
            node.getTerms().stream().map(n -> process(n, context)).collect(Collectors.toList()));
      case AND:
        return FilterFactory.and(
            node.getTerms().stream().map(n -> process(n, context)).collect(Collectors.toList()));
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported logical operator %s", node.getOperator()));
    }
  }

  @Override
  protected Filter visitNotExpression(NotExpression node, Context context) {
    return FilterFactory.not(process(node.getValue(), context));
  }

  @Override
  protected Filter visitComparisonExpression(ComparisonExpression node, Context context) {
    if (isTimeColumn(node.getLeft()) || isTimeColumn(node.getRight())) {
      return TIME_FILTER_VISITOR.process(node, null);
    }

    Expression left = node.getLeft();
    Expression right = node.getRight();

    if (isSymbolReference(left)
        && context.isMeasurementColumn((SymbolReference) left)
        && isLiteral(right)) {
      return constructCompareFilter(
          node.getOperator(), (SymbolReference) left, (Literal) right, context);
    } else if (isLiteral(left)
        && isSymbolReference(right)
        && context.isMeasurementColumn((SymbolReference) right)) {
      return constructCompareFilter(
          node.getOperator(), (SymbolReference) right, (Literal) left, context);
    } else {
      throw new IllegalStateException(
          String.format("%s is not supported in value push down", node));
    }
  }

  @Override
  protected Filter visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
    throw new UnsupportedOperationException("Filter push down does not support CASE WHEN");
  }

  @Override
  protected Filter visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
    throw new UnsupportedOperationException("Filter push down does not support CASE WHEN");
  }

  @Override
  protected Filter visitIfExpression(IfExpression node, Context context) {
    throw new UnsupportedOperationException("Filter push down does not support IF");
  }

  @Override
  protected Filter visitNullIfExpression(NullIfExpression node, Context context) {
    throw new UnsupportedOperationException("Filter push down does not support NULLIF");
  }

  @Override
  protected Filter visitBetweenPredicate(BetweenPredicate node, Context context) {
    Expression firstExpression = node.getValue();
    Expression secondExpression = node.getMin();
    Expression thirdExpression = node.getMax();

    if (isTimeColumn(firstExpression)
        || isTimeColumn(secondExpression)
        || isTimeColumn(thirdExpression)) {
      return TIME_FILTER_VISITOR.process(node, null);
    }

    if (isSymbolReference(firstExpression)
        && context.isMeasurementColumn((SymbolReference) firstExpression)) {
      return constructBetweenFilter(
          (SymbolReference) firstExpression, secondExpression, thirdExpression, context);
    } else if (isSymbolReference(secondExpression)
        && context.isMeasurementColumn((SymbolReference) secondExpression)) {
      checkArgument(isLiteral(firstExpression));
      return constructCompareFilter(
          ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
          (SymbolReference) secondExpression,
          (Literal) firstExpression,
          context);
    } else if (isSymbolReference(thirdExpression)
        && context.isMeasurementColumn((SymbolReference) thirdExpression)) {
      checkArgument(isLiteral(firstExpression));
      return constructCompareFilter(
          ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
          (SymbolReference) thirdExpression,
          (Literal) firstExpression,
          context);
    } else {
      throw new IllegalStateException(
          String.format("%s is not supported in value push down", node));
    }
  }

  private <T extends Comparable<T>> Filter constructBetweenFilter(
      SymbolReference measurementReference,
      Expression minValue,
      Expression maxValue,
      ConvertPredicateToFilterVisitor.Context context) {
    int measurementIndex = context.getMeasurementIndex(measurementReference.getName());
    Type dataType = context.getType(Symbol.from(measurementReference));

    checkArgument(isLiteral(minValue) && isLiteral(maxValue));

    T min = getValue((Literal) minValue, dataType);
    T max = getValue((Literal) maxValue, dataType);

    if (min.compareTo(max) == 0) {
      return ValueFilterApi.eq(measurementIndex, min);
    }
    return ValueFilterApi.between(measurementIndex, min, max);
  }

  public static double getDoubleValue(Expression expression) {
    return ((DoubleLiteral) expression).getValue();
  }

  public static boolean getBooleanValue(Expression expression) {
    return ((BooleanLiteral) expression).getValue();
  }

  public static String getStringValue(Expression expression) {
    return ((StringLiteral) expression).getValue();
  }

  public static class Context {

    private final Map<String, Integer> measuremrntsMap;
    private final Map<Symbol, ColumnSchema> schemaMap;

    public Context(List<String> allMeasurements, Map<Symbol, ColumnSchema> schemaMap) {
      this.measuremrntsMap = new HashMap<>(allMeasurements.size());
      for (int i = 0, size = allMeasurements.size(); i < size; i++) {
        measuremrntsMap.put(allMeasurements.get(i), i);
      }
      this.schemaMap = schemaMap;
    }

    public int getMeasurementIndex(String measurement) {
      Integer index = measuremrntsMap.get(measurement);
      if (index == null) {
        throw new IllegalArgumentException(
            String.format("Measurement %s does not exist", measurement));
      }
      return index;
    }

    public Type getType(Symbol symbol) {
      Type type = schemaMap.get(symbol).getType();
      if (type == null) {
        throw new IllegalArgumentException(
            String.format("ColumnSchema of Symbol %s isn't saved in schemaMap", symbol));
      }
      return type;
    }

    public boolean isMeasurementColumn(SymbolReference symbolReference) {
      ColumnSchema schema = schemaMap.get(Symbol.from(symbolReference));
      return schema != null && schema.getColumnCategory() == TsTableColumnCategory.MEASUREMENT;
    }
  }
}
