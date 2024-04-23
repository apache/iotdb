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

import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import static com.google.common.collect.ImmutableList.toImmutableList;

/** Change Identifier to SymbolReference */
public class ExpressionTranslateVisitor extends AstVisitor<Expression, PlanBuilder> {

  @Override
  protected Expression visitSymbolReference(SymbolReference node, PlanBuilder context) {
    return new SymbolReference(context.getSymbolForColumn(node).get().getName());
  }

  @Override
  protected Expression visitIdentifier(Identifier node, PlanBuilder context) {
    return context.getSymbolForColumn(node).map(Symbol::toSymbolReference).get();
  }

  @Override
  protected Expression visitInPredicate(InPredicate node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitIsNullPredicate(IsNullPredicate node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitLikePredicate(LikePredicate node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitLogicalExpression(LogicalExpression node, PlanBuilder context) {
    return new LogicalExpression(
        node.getOperator(),
        node.getTerms().stream().map(e -> process(e, context)).collect(toImmutableList()));
  }

  @Override
  protected Expression visitNotExpression(NotExpression node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitComparisonExpression(ComparisonExpression node, PlanBuilder context) {
    Expression left = process(node.getLeft(), context);
    Expression right = process(node.getRight(), context);
    return new ComparisonExpression(node.getOperator(), left, right);
  }

  @Override
  protected Expression visitSimpleCaseExpression(SimpleCaseExpression node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitSearchedCaseExpression(
      SearchedCaseExpression node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitIfExpression(IfExpression node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitNullIfExpression(NullIfExpression node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitBetweenPredicate(BetweenPredicate node, PlanBuilder context) {
    return null;
  }

  @Override
  protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, PlanBuilder context) {
    return new ArithmeticBinaryExpression(
        node.getOperator(), process(node.getLeft(), context), process(node.getRight(), context));
  }

  @Override
  protected Expression visitLiteral(Literal node, PlanBuilder context) {
    return node;
  }
}
