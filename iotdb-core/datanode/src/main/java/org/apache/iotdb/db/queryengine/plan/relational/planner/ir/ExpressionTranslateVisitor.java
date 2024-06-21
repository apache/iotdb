/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TranslationMap;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

/** Change Identifier to SymbolReference */
public class ExpressionTranslateVisitor extends RewritingVisitor<TranslationMap> {

  private ExpressionTranslateVisitor() {}

  private static final String NOT_SUPPORTED = "%s is not supported expression translate yet.";

  public static Expression translateToSymbolReference(
      Expression expression, TranslationMap translations) {
    return new ExpressionTranslateVisitor().process(expression, translations);
  }

  @Override
  protected Expression visitExpression(Expression node, TranslationMap context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass().getName()));
  }

  @Override
  protected Expression visitSymbolReference(SymbolReference node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new SymbolReference(context.getSymbolForColumn(node).get().getName());
  }

  @Override
  protected Expression visitIdentifier(Identifier node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return context.getSymbolForColumn(node).map(Symbol::toSymbolReference).get();
  }

  @Override
  protected Expression visitArithmeticBinary(
      ArithmeticBinaryExpression node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new ArithmeticBinaryExpression(
        node.getOperator(), process(node.getLeft(), context), process(node.getRight(), context));
  }

  @Override
  protected Expression visitLogicalExpression(LogicalExpression node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new LogicalExpression(
        node.getOperator(),
        node.getTerms().stream().map(e -> process(e, context)).collect(toImmutableList()));
  }

  @Override
  protected Expression visitLiteral(Literal node, TranslationMap context) {
    return node;
  }

  @Override
  protected Expression visitComparisonExpression(
      ComparisonExpression node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    Expression left = process(node.getLeft(), context);
    Expression right = process(node.getRight(), context);
    return new ComparisonExpression(node.getOperator(), left, right);
  }

  @Override
  protected Expression visitFunctionCall(FunctionCall node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    List<Expression> newArguments =
        node.getArguments().stream()
            .map(argument -> process(argument, context))
            .collect(Collectors.toList());
    return new FunctionCall(node.getName(), newArguments);
  }

  @Override
  protected Expression visitIsNullPredicate(IsNullPredicate node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new IsNullPredicate(process(node.getValue(), context));
  }

  @Override
  protected Expression visitInPredicate(InPredicate node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new InPredicate(
        process(node.getValue(), context), process(node.getValueList(), context));
  }

  @Override
  protected Expression visitInListExpression(InListExpression node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    List<Expression> newValues =
        node.getValues().stream().map(this::process).collect(Collectors.toList());
    return new InListExpression(newValues);
  }

  @Override
  protected Expression visitLikePredicate(LikePredicate node, TranslationMap context) {
    Optional<SymbolReference> mapped = context.tryGetMapping(node);
    if (mapped.isPresent()) {
      return mapped.get();
    }
    return new LikePredicate(
        process(node.getValue(), context),
        process(node.getPattern(), context),
        process(node.getPattern(), context));
  }

  // ============================ not implemented =======================================

  @Override
  protected Expression visitSimpleCaseExpression(
      SimpleCaseExpression node, TranslationMap context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass().getName()));
  }

  @Override
  protected Expression visitSearchedCaseExpression(
      SearchedCaseExpression node, TranslationMap context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass().getName()));
  }

  @Override
  protected Expression visitIfExpression(IfExpression node, TranslationMap context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass().getName()));
  }

  @Override
  protected Expression visitNullIfExpression(NullIfExpression node, TranslationMap context) {
    throw new IllegalStateException(String.format(NOT_SUPPORTED, node.getClass().getName()));
  }
}
