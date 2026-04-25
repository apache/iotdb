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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DecimalLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;

import java.util.List;
import java.util.Set;

/**
 * only the following predicate will return true: 1. tagColumn = 'XXX' 2. 'XXX' = tagColumn 3.
 * attributeColumn = 'XXX' 4. 'XXX' = attributeColumn 5. tagColumn/attributeColumn IS NULL 6. using
 * or to combine the above expression
 */
public class PredicatePushIntoMetadataChecker implements AstVisitor<Boolean, Void> {

  private final Set<String> idOrAttributeColumnNames;

  public static boolean check(
      final Set<String> idOrAttributeColumnNames, final Expression expression) {
    return new PredicatePushIntoMetadataChecker(idOrAttributeColumnNames).process(expression);
  }

  public PredicatePushIntoMetadataChecker(final Set<String> idOrAttributeColumnNames) {
    this.idOrAttributeColumnNames = idOrAttributeColumnNames;
  }

  @Override
  public Boolean visitExpression(final Expression expression, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitArithmeticBinary(final ArithmeticBinaryExpression node, final Void context) {
    return node.getLeft().accept(this, context) && node.getRight().accept(this, context);
  }

  @Override
  public Boolean visitArithmeticUnary(final ArithmeticUnaryExpression node, final Void context) {
    return node.getValue().accept(this, context);
  }

  @Override
  public Boolean visitBetweenPredicate(final BetweenPredicate node, final Void context) {
    return node.getValue().accept(this, context)
        && node.getMin().accept(this, context)
        && node.getMax().accept(this, context);
  }

  @Override
  public Boolean visitCast(final Cast node, final Void context) {
    return node.getExpression().accept(this, context);
  }

  @Override
  public Boolean visitBooleanLiteral(final BooleanLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitBinaryLiteral(final BinaryLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitStringLiteral(final StringLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitLongLiteral(final LongLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitDoubleLiteral(final DoubleLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitFloatLiteral(final FloatLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitDecimalLiteral(final DecimalLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitGenericLiteral(final GenericLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitNullLiteral(final NullLiteral node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitComparisonExpression(final ComparisonExpression node, final Void context) {
    return node.getLeft().accept(this, context) && node.getRight().accept(this, context);
  }

  @Override
  public Boolean visitCurrentDatabase(final CurrentDatabase node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitCurrentTime(final CurrentTime node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitCurrentUser(final CurrentUser node, final Void context) {
    return true;
  }

  @Override
  public Boolean visitFunctionCall(final FunctionCall node, final Void context) {
    return node.getArguments().stream().allMatch(expression -> expression.accept(this, context));
  }

  @Override
  public Boolean visitInPredicate(final InPredicate node, final Void context) {
    return node.getValue().accept(this, context) && node.getValueList().accept(this, context);
  }

  @Override
  public Boolean visitInListExpression(final InListExpression node, final Void context) {
    return node.getValues().stream().allMatch(expression -> expression.accept(this, context));
  }

  @Override
  public Boolean visitIsNullPredicate(final IsNullPredicate node, final Void context) {
    return node.getValue().accept(this, context);
  }

  @Override
  public Boolean visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
    return node.getValue().accept(this, context);
  }

  @Override
  public Boolean visitLikePredicate(final LikePredicate node, final Void context) {
    return node.getValue().accept(this, context);
  }

  @Override
  public Boolean visitLogicalExpression(final LogicalExpression node, final Void context) {
    final List<Expression> children = node.getTerms();
    for (final Expression child : children) {
      final Boolean result = process(child, context);
      if (result == null) {
        throw new IllegalStateException("Should never return null.");
      }
      if (!result) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean visitNotExpression(final NotExpression node, final Void context) {
    if (node.getValue() instanceof Literal) {
      return Boolean.FALSE;
    }
    return node.getValue().accept(this, context);
  }

  @Override
  public Boolean visitSymbolReference(final SymbolReference node, final Void context) {
    return idOrAttributeColumnNames.contains(node.getName());
  }

  @Override
  public Boolean visitCoalesceExpression(final CoalesceExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitSimpleCaseExpression(final SimpleCaseExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitTrim(final Trim node, final Void context) {
    return node.getTrimSource().accept(this, context)
        && (!node.getTrimCharacter().isPresent()
            || node.getTrimCharacter().orElse(null).accept(this, context));
  }

  @Override
  public Boolean visitIfExpression(final IfExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitNullIfExpression(final NullIfExpression node, final Void context) {
    return Boolean.FALSE;
  }
}
