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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

public class PredicateCombineIntoTableScanChecker extends PredicateVisitor<Boolean, Void> {

  private final Set<String> timeOrMeasurementColumns;

  public static boolean check(Set<String> measurementColumns, Expression expression) {
    return new PredicateCombineIntoTableScanChecker(measurementColumns).process(expression);
  }

  public PredicateCombineIntoTableScanChecker(Set<String> timeOrMeasurementColumns) {
    this.timeOrMeasurementColumns = timeOrMeasurementColumns;
  }

  @Override
  public Boolean visitExpression(Expression expression, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitNotExpression(NotExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitInPredicate(InPredicate node, Void context) {
    return isTimeOrMeasurementColumn(node.getValue()) && isInListAllLiteral(node);
  }

  public static Boolean isInListAllLiteral(InPredicate node) {
    if (node.getValueList() instanceof InListExpression) {
      List<Expression> values = ((InListExpression) node.getValueList()).getValues();
      for (Expression value : values) {
        if (!(value instanceof Literal)) {
          return Boolean.FALSE;
        }
      }
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  @Override
  protected Boolean visitLikePredicate(LikePredicate node, Void context) {
    return isTimeOrMeasurementColumn(node.getValue())
        && isLiteral(node.getPattern())
        && node.getEscape().map(PredicatePushIntoScanChecker::isLiteral).orElse(true);
  }

  @Override
  protected Boolean visitLogicalExpression(LogicalExpression node, Void context) {
    List<Expression> children = node.getTerms();
    for (Expression child : children) {
      Boolean result = process(child, context);
      if (result == null) {
        throw new IllegalStateException(
            "Should never return null in PredicateCombineIntoTableScanChecker.");
      }
      if (!result) {
        return Boolean.FALSE;
      }
    }

    return Boolean.TRUE;
  }

  @Override
  protected Boolean visitComparisonExpression(ComparisonExpression node, Void context) {
    return (isTimeOrMeasurementColumn(node.getLeft()) && isLiteral(node.getRight()))
        || (isTimeOrMeasurementColumn(node.getRight()) && isLiteral(node.getLeft()))
        || (isExtractTimeOrMeasurementColumn(node.getLeft()) && isLiteral(node.getRight()))
        || (isExtractTimeOrMeasurementColumn(node.getRight()) && isLiteral(node.getLeft()));
  }

  @Override
  protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context) {
    return (isTimeOrMeasurementColumn(node.getValue())
            && isLiteral(node.getMin())
            && isLiteral(node.getMax()))
        || (isExtractTimeOrMeasurementColumn(node.getValue())
            && isLiteral(node.getMin())
            && isLiteral(node.getMax()));
    // TODO After Constant-Folding introduced
    /*|| (isLiteral(node.getValue())
        && isTimeOrMeasurementColumn(node.getMin())
        && isLiteral(node.getMax()))
    || (isLiteral(node.getValue())
        && isLiteral(node.getMin())
        && isTimeOrMeasurementColumn(node.getMax()));*/
  }

  @Override
  protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    return isTimeOrMeasurementColumn(node.getValue());
  }

  // expression below will be supported later
  @Override
  protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIfExpression(IfExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitNullIfExpression(NullIfExpression node, Void context) {
    return Boolean.FALSE;
  }

  private boolean isTimeOrMeasurementColumn(Expression expression) {
    return isSymbolReference(expression)
        && timeOrMeasurementColumns.contains(((SymbolReference) expression).getName());
  }

  private boolean isExtractTimeOrMeasurementColumn(Expression expression) {
    return expression instanceof Extract
        && ((Extract) expression).getExpression() instanceof SymbolReference
        && timeOrMeasurementColumns.contains(
            ((SymbolReference) ((Extract) expression).getExpression()).getName());
  }
}
