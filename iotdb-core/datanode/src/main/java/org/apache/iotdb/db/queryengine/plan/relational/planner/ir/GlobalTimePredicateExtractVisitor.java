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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.expression.UnknownExpressionTypeException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
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

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.OR;

public class GlobalTimePredicateExtractVisitor
    extends AstVisitor<Pair<Expression, Boolean>, GlobalTimePredicateExtractVisitor.Context> {

  private static final String NOT_SUPPORTED =
      "visit() not implemented for %s in GlobalTimePredicateExtract.";

  /**
   * Extract global time predicate from query predicate.
   *
   * @param predicate raw query predicate
   * @return Pair, left is globalTimePredicate, right is if hasValueFilter.
   * @throws UnknownExpressionTypeException unknown expression type
   */
  public static Pair<Expression, Boolean> extractGlobalTimeFilter(
      Expression predicate, String timeColumnName) {
    return new GlobalTimePredicateExtractVisitor()
        .process(
            predicate, new GlobalTimePredicateExtractVisitor.Context(true, true, timeColumnName));
  }

  protected Pair<Expression, Boolean> visitExpression(
      Pair<Expression, Boolean> node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitLogicalExpression(
      LogicalExpression node, Context context) {
    if (node.getOperator() == AND) {
      List<Pair<Expression, Boolean>> resultPairs = new ArrayList<>();
      for (Expression term : node.getTerms()) {
        resultPairs.add(process(term, context));
      }

      List<Expression> newTimeFilterTerms = new ArrayList<>();
      List<Expression> newValueFilterTerms = new ArrayList<>();
      // rewrite predicate to avoid duplicate calculation on time filter
      // If Left-child or Right-child does not contain value filter
      // We can set it to true in Predicate Tree
      if (context.canRewrite) {
        getNewTimeValueExpressions(node, resultPairs, newTimeFilterTerms, newValueFilterTerms);
      }

      if (!newTimeFilterTerms.isEmpty()) {
        node.setTerms(newValueFilterTerms);

        return new Pair<>(
            newTimeFilterTerms.size() == 1
                ? newTimeFilterTerms.get(0)
                : new LogicalExpression(AND, newTimeFilterTerms),
            !newValueFilterTerms.isEmpty());
      }

      return new Pair<>(null, true);
    } else if (node.getOperator() == OR) {

      List<Pair<Expression, Boolean>> resultPairs = new ArrayList<>();
      for (Expression term : node.getTerms()) {
        resultPairs.add(process(term, new Context(false, false, context.timeColumnName)));
      }

      List<Expression> newTimeFilterTerms = new ArrayList<>();
      List<Expression> newValueFilterTerms = new ArrayList<>();

      getNewTimeValueExpressions(node, resultPairs, newTimeFilterTerms, newValueFilterTerms);

      // for example, `(t1 and s1) or t2`, `t1 or t2` meets this condition
      if (newTimeFilterTerms.size() == node.getTerms().size()) {
        if (context.isFirstOr && newValueFilterTerms.isEmpty()) {
          node.setTerms(Collections.singletonList(TRUE_LITERAL));
        }
        return new Pair<>(
            newTimeFilterTerms.size() == 1
                ? newTimeFilterTerms.get(0)
                : new LogicalExpression(OR, newTimeFilterTerms),
            !newValueFilterTerms.isEmpty());
      }

      return new Pair<>(null, true);
    } else {
      throw new IllegalStateException("Illegal state in visitLogicalExpression");
    }
  }

  private void getNewTimeValueExpressions(
      LogicalExpression node,
      List<Pair<Expression, Boolean>> resultPairs,
      List<Expression> newTimeFilterTerms,
      List<Expression> newValueFilterTerms) {
    for (int i = 0; i < resultPairs.size(); i++) {
      Pair<Expression, Boolean> pair = resultPairs.get(i);

      if (pair.left != null) {
        newTimeFilterTerms.add(pair.left);

        // has time filter, also has value filter
        if (pair.right) {
          newValueFilterTerms.add(node.getTerms().get(i));
        }
      } else {
        // only has value filter
        newValueFilterTerms.add(node.getTerms().get(i));
      }
    }
  }

  @Override
  protected Pair<Expression, Boolean> visitComparisonExpression(
      ComparisonExpression node, Context context) {
    Expression leftExpression = node.getLeft();
    Expression rightExpression = node.getRight();
    if (checkIsTimeFilter(leftExpression, context.timeColumnName, rightExpression)
        || checkIsTimeFilter(rightExpression, context.timeColumnName, leftExpression)) {
      return new Pair<>(node, false);
    }
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitIsNullPredicate(IsNullPredicate node, Context context) {
    // time filter don't support IS_NULL
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitIsNotNullPredicate(
      IsNotNullPredicate node, Context context) {
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitFunctionCall(FunctionCall node, Context context) {
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitLikePredicate(LikePredicate node, Context context) {
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitBetweenPredicate(
      BetweenPredicate node, Context context) {
    Expression firstExpression = node.getValue();
    Expression secondExpression = node.getMin();
    Expression thirdExpression = node.getMax();

    boolean isTimeFilter = false;
    if (isTimeColumn(firstExpression, context.timeColumnName)) {
      isTimeFilter = checkBetweenConstantSatisfy(secondExpression, thirdExpression);
    }
    // TODO After Constant-Folding introduced
    /*else if (isTimeIdentifier(secondExpression)) {
      isTimeFilter = checkBetweenConstantSatisfy(firstExpression, thirdExpression);
    } else if (isTimeIdentifier(thirdExpression)) {
      isTimeFilter = checkBetweenConstantSatisfy(secondExpression, firstExpression);
    }*/
    if (isTimeFilter) {
      return new Pair<>(node, false);
    }
    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitInPredicate(InPredicate node, Context context) {
    if (isTimeColumn(node.getValue(), context.timeColumnName)) {
      return new Pair<>(node, false);
    }

    return new Pair<>(null, true);
  }

  @Override
  protected Pair<Expression, Boolean> visitNotExpression(NotExpression node, Context context) {
    Pair<Expression, Boolean> result =
        process(
            node.getValue(),
            new Context(context.canRewrite, context.isFirstOr, context.timeColumnName));
    if (result.left != null) {
      return new Pair<>(new NotExpression(result.left), result.right);
    }
    return new Pair<>(null, true);
  }

  // ============================ not implemented =======================================

  @Override
  protected Pair<Expression, Boolean> visitArithmeticBinary(
      ArithmeticBinaryExpression node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitSimpleCaseExpression(
      SimpleCaseExpression node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitSearchedCaseExpression(
      SearchedCaseExpression node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitIfExpression(IfExpression node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitNullIfExpression(
      NullIfExpression node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  public static boolean isTimeColumn(Expression e, String timeColumnName) {
    return e instanceof SymbolReference
        && ((SymbolReference) e).getName().equalsIgnoreCase(timeColumnName);
  }

  private static boolean checkIsTimeFilter(
      Expression timeExpression, String timeColumnName, Expression valueExpression) {
    return isTimeColumn(timeExpression, timeColumnName) && valueExpression instanceof LongLiteral;
  }

  private static boolean checkBetweenConstantSatisfy(Expression e1, Expression e2) {
    return e1 instanceof LongLiteral
        && e2 instanceof LongLiteral
        && ((LongLiteral) e1).getParsedValue() <= ((LongLiteral) e2).getParsedValue();
  }

  public static class Context {
    boolean canRewrite;
    boolean isFirstOr;
    String timeColumnName;

    public Context(boolean canRewrite, boolean isFirstOr, String timeColumnName) {
      this.canRewrite = canRewrite;
      this.isFirstOr = isFirstOr;
      this.timeColumnName = timeColumnName;
    }
  }
}
