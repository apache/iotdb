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

import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.LongLiteral;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;

import org.apache.tsfile.utils.Pair;

import java.util.Arrays;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;
import static org.apache.iotdb.db.relational.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.relational.sql.tree.LogicalExpression.Operator.AND;
import static org.apache.iotdb.db.relational.sql.tree.LogicalExpression.Operator.OR;
import static org.apache.iotdb.db.relational.sql.tree.LogicalExpression.and;
import static org.apache.iotdb.db.relational.sql.tree.LogicalExpression.or;

public class GlobalTimePredicateExtractVisitor
    extends IrVisitor<Pair<Expression, Boolean>, GlobalTimePredicateExtractVisitor.Context> {

  private static final String NOT_SUPPORTED =
      "visit() not implemented for %s in GlobalTimePredicateExtract.";

  public static Pair<Expression, Boolean> extractGlobalTimeFilter(Expression predicate) {
    return new GlobalTimePredicateExtractVisitor()
        .process(predicate, new GlobalTimePredicateExtractVisitor.Context(true, true));
  }

  protected Pair<Expression, Boolean> visitExpression(
      Pair<Expression, Boolean> node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitLogicalExpression(
      LogicalExpression node, Context context) {
    if (node.getOperator() == AND) {
      Pair<Expression, Boolean> leftResultPair = process(node.getTerms().get(0), context);
      Pair<Expression, Boolean> rightResultPair = process(node.getTerms().get(1), context);

      // rewrite predicate to avoid duplicate calculation on time filter
      // If Left-child or Right-child does not contain value filter
      // We can set it to true in Predicate Tree
      if (context.canRewrite) {
        Expression newLeftExpression = null, newRightExpression = null;
        if (leftResultPair.left != null && !leftResultPair.right) {
          newLeftExpression = TRUE_LITERAL;
        }
        if (rightResultPair.left != null && !rightResultPair.right) {
          newRightExpression = TRUE_LITERAL;
        }
        if (newLeftExpression != null || newRightExpression != null) {
          node.setTerms(
              Arrays.asList(
                  newLeftExpression != null ? newLeftExpression : node.getTerms().get(0),
                  newRightExpression != null ? newRightExpression : node.getTerms().get(1)));
        }
      }

      if (leftResultPair.left != null && rightResultPair.left != null) {
        return new Pair<>(
            and(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      } else if (leftResultPair.left != null) {
        return new Pair<>(leftResultPair.left, true);
      } else if (rightResultPair.left != null) {
        return new Pair<>(rightResultPair.left, true);
      }

      return new Pair<>(null, true);
    } else if (node.getOperator() == OR) {
      Pair<Expression, Boolean> leftResultPair =
          process(node.getTerms().get(0), new Context(false, false));
      Pair<Expression, Boolean> rightResultPair =
          process(node.getTerms().get(1), new Context(false, false));

      if (leftResultPair.left != null && rightResultPair.left != null) {
        if (Boolean.TRUE.equals(
            context.isFirstOr && !leftResultPair.right && !rightResultPair.right)) {
          node.getTerms().set(0, TRUE_LITERAL);
          node.getTerms().set(0, TRUE_LITERAL);
        }
        return new Pair<>(
            or(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      }
      return new Pair<>(null, true);
    } else {
      throw new IllegalStateException("Illegal state in visitLogicalExpression");
    }
  }

  @Override
  protected Pair<Expression, Boolean> visitComparisonExpression(
      ComparisonExpression node, Context context) {
    Expression leftExpression = node.getLeft();
    Expression rightExpression = node.getRight();
    if (checkIsTimeFilter(leftExpression, rightExpression)
        || checkIsTimeFilter(rightExpression, leftExpression)) {
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
    if (isTimeIdentifier(firstExpression)) {
      isTimeFilter = checkBetweenConstantSatisfy(secondExpression, thirdExpression);
    } else if (isTimeIdentifier(secondExpression)) {
      isTimeFilter = checkBetweenConstantSatisfy(firstExpression, thirdExpression);
    } else if (isTimeIdentifier(thirdExpression)) {
      isTimeFilter = checkBetweenConstantSatisfy(secondExpression, firstExpression);
    }
    if (isTimeFilter) {
      return new Pair<>(node, false);
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
  protected Pair<Expression, Boolean> visitInPredicate(InPredicate node, Context context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass()));
  }

  @Override
  protected Pair<Expression, Boolean> visitNotExpression(NotExpression node, Context context) {
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

  private static boolean isTimeIdentifier(Expression e) {
    return e instanceof Identifier && TIME.equalsIgnoreCase(((Identifier) e).getValue());
  }

  private static boolean checkIsTimeFilter(Expression timeExpression, Expression valueExpression) {
    return timeExpression instanceof Identifier
        && ((Identifier) timeExpression).getValue().equalsIgnoreCase(TIME)
        && valueExpression instanceof LongLiteral;
  }

  private static boolean checkBetweenConstantSatisfy(Expression e1, Expression e2) {
    return e1 instanceof LongLiteral
        && e2 instanceof LongLiteral
        && ((LongLiteral) e1).getParsedValue() <= ((LongLiteral) e2).getParsedValue();
  }

  public static class Context {
    boolean canRewrite;
    boolean isFirstOr;

    public Context(boolean canRewrite, boolean isFirstOr) {
      this.canRewrite = canRewrite;
      this.isFirstOr = isFirstOr;
    }
  }
}
