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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/** The caller must make sure that the Expression only contains valid time predicate */
public class ConvertPredicateToTimeFilterVisitor extends PredicateVisitor<Filter, Void> {

  @Override
  protected Filter visitInPredicate(InPredicate node, Void context) {
    Expression valueList = node.getValueList();
    checkArgument(valueList instanceof InListExpression);
    List<Expression> values = ((InListExpression) valueList).getValues();
    for (Expression value : values) {
      checkArgument(value instanceof LongLiteral);
    }
    if (values.size() == 1) {
      return TimeFilterApi.eq(getLongValue(values.get(0)));
    }
    Set<Long> longValues = new HashSet<>();
    for (Expression value : values) {
      longValues.add(((LongLiteral) value).getParsedValue());
    }
    return TimeFilterApi.in(longValues);
  }

  @Override
  protected Filter visitIsNullPredicate(IsNullPredicate node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support IS NULL");
  }

  @Override
  protected Filter visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support IS NOT NULL");
  }

  @Override
  protected Filter visitLikePredicate(LikePredicate node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support LIKE");
  }

  @Override
  protected Filter visitLogicalExpression(LogicalExpression node, Void context) {
    List<Filter> filterList =
        node.getTerms().stream()
            .map(n -> n.accept(this, context))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    switch (node.getOperator()) {
      case OR:
        return FilterFactory.or(filterList);
      case AND:
        return FilterFactory.and(filterList);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + node.getOperator());
    }
  }

  @Override
  protected Filter visitNotExpression(NotExpression node, Void context) {
    return FilterFactory.not(node.getValue().accept(this, context));
  }

  @Override
  protected Filter visitComparisonExpression(ComparisonExpression node, Void context) {
    long value;
    if (node.getLeft() instanceof LongLiteral) {
      value = getLongValue(node.getLeft());
      switch (node.getOperator()) {
        case EQUAL:
          return TimeFilterApi.eq(value);
        case NOT_EQUAL:
          return TimeFilterApi.notEq(value);
        case GREATER_THAN:
          return TimeFilterApi.lt(value);
        case GREATER_THAN_OR_EQUAL:
          return TimeFilterApi.ltEq(value);
        case LESS_THAN:
          return TimeFilterApi.gt(value);
        case LESS_THAN_OR_EQUAL:
          return TimeFilterApi.gtEq(value);
        default:
          throw new IllegalArgumentException("Unsupported operator: " + node.getOperator());
      }
    } else if (node.getRight() instanceof LongLiteral) {
      value = getLongValue(node.getRight());
      switch (node.getOperator()) {
        case EQUAL:
          return TimeFilterApi.eq(value);
        case NOT_EQUAL:
          return TimeFilterApi.notEq(value);
        case GREATER_THAN:
          return TimeFilterApi.gt(value);
        case GREATER_THAN_OR_EQUAL:
          return TimeFilterApi.gtEq(value);
        case LESS_THAN:
          return TimeFilterApi.lt(value);
        case LESS_THAN_OR_EQUAL:
          return TimeFilterApi.ltEq(value);
        default:
          throw new IllegalArgumentException("Unsupported operator: " + node.getOperator());
      }
    } else {
      throw new IllegalStateException(
          "Either left or right operand of Time ComparisonExpression should be LongLiteral");
    }
  }

  @Override
  protected Filter visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not CASE WHEN");
  }

  @Override
  protected Filter visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not CASE WHEN");
  }

  @Override
  protected Filter visitIfExpression(IfExpression node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not IF");
  }

  @Override
  protected Filter visitNullIfExpression(NullIfExpression node, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not NULLIF");
  }

  @Override
  protected Filter visitBetweenPredicate(BetweenPredicate node, Void context) {
    Expression firstExpression = node.getValue();
    Expression secondExpression = node.getMin();
    Expression thirdExpression = node.getMax();

    if (firstExpression instanceof SymbolReference) {
      // firstExpression is TIMESTAMP
      long minValue = getLongValue(secondExpression);
      long maxValue = getLongValue(thirdExpression);

      if (minValue == maxValue) {
        return TimeFilterApi.eq(minValue);
      }
      return TimeFilterApi.between(minValue, maxValue);
    } else if (secondExpression instanceof SymbolReference) {
      // secondExpression is TIMESTAMP
      long value = getLongValue(firstExpression);
      long maxValue = getLongValue(thirdExpression);

      // cases:
      // 1 BETWEEN time AND 2 => time <= 1
      // 1 BETWEEN time AND 1 => time <= 1
      // 1 BETWEEN time AND 0 => FALSE
      // 1 NOT BETWEEN time AND 2 => time > 1
      // 1 NOT BETWEEN time AND 1 => time > 1
      // 1 NOT BETWEEN time AND 0 => TRUE
      checkArgument(
          value <= maxValue,
          String.format("Predicate [%s] should be simplified in previous step", node));
      return TimeFilterApi.ltEq(value);
    } else if (thirdExpression instanceof SymbolReference) {
      // thirdExpression is TIMESTAMP
      long value = getLongValue(firstExpression);
      long minValue = getLongValue(secondExpression);

      // cases:
      // 1 BETWEEN 2 AND time => FALSE
      // 1 BETWEEN 1 AND time => time >= 1
      // 1 BETWEEN 0 AND time => time >= 1
      // 1 NOT BETWEEN 2 AND time => TRUE
      // 1 NOT BETWEEN 1 AND time => time < 1
      // 1 NOT BETWEEN 0 AND time => time < 1
      checkArgument(
          value >= minValue,
          String.format("Predicate [%s] should be simplified in previous step", node));
      return TimeFilterApi.gtEq(value);
    } else {
      throw new IllegalStateException(
          "Three operand of between expression should have time column.");
    }
  }

  public static long getLongValue(Expression expression) {
    return ((LongLiteral) expression).getParsedValue();
  }
}
