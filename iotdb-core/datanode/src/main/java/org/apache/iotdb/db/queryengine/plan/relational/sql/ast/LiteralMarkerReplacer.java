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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.util.LinkedList;
import java.util.List;

public class LiteralMarkerReplacer extends AstVisitor<Void, Void> {

  private static final String NOT_SUPPORTED =
      "visit() not supported for %s in LiteralMarkerReplacer.";

  List<Literal> literalList = new LinkedList<Literal>();
  int literalGlobalIndex = -1;

  public List<Literal> getLiteralList() {
    return literalList;
  }

  @Override
  protected Void visitNode(Node node, Void context) {
    throw new UnsupportedOperationException(
        "visitNode of Node {" + node.getClass() + "} is not supported in LiteralMarkerReplacer");
  }

  @Override
  protected Void visitQuery(Query node, Void context) {
    node.getQueryBody().accept(this, context);
    return context;
  }

  @Override
  protected Void visitQuerySpecification(QuerySpecification node, Void context) {
    node.getSelect().accept(this, context);
    node.getWhere().ifPresent(where -> where.accept(this, context));
    node.getGroupBy().ifPresent(groupBy -> groupBy.accept(this, context));
    node.getHaving().ifPresent(having -> having.accept(this, context));
    node.getOrderBy().ifPresent(orderBy -> orderBy.accept(this, context));
    node.getLimit().ifPresent(limit -> limit.accept(this, context));
    node.getOffset().ifPresent(offset -> offset.accept(this, context));
    return context;
  }

  @Override
  protected Void visitSelect(Select node, Void context) {
    node.getSelectItems().forEach(item -> item.accept(this, context));
    return context;
  }

  @Override
  protected Void visitAllColumns(AllColumns node, Void context) {
    node.getTarget().ifPresent(target -> target.accept(this, context));
    return context;
  }

  @Override
  protected Void visitSingleColumn(SingleColumn node, Void context) {
    node.getExpression().accept(this, context);
    return context;
  }

  @Override
  protected Void visitOrderBy(OrderBy node, Void context) {
    node.getSortItems().forEach(item -> item.getSortKey().accept(this, context));
    return context;
  }

  @Override
  protected Void visitGroupBy(GroupBy node, Void context) {
    for (GroupingElement element : node.getGroupingElements()) {
      element.getExpressions().forEach(expression -> expression.accept(this, context));
    }
    return context;
  }

  @Override
  protected Void visitLimit(Limit node, Void context) {
    node.getRowCount().accept(this, context);
    return context;
  }

  @Override
  protected Void visitOffset(Offset node, Void context) {
    node.getRowCount().accept(this, context);
    return context;
  }

  // ============================ Expression ============================

  @Override
  protected Void visitExpression(Expression node, Void context) {
    // do nothing
    return context;
  }

  @Override
  protected Void visitLogicalExpression(LogicalExpression node, Void context) {
    node.getTerms().forEach(term -> term.accept(this, context));
    return context;
  }

  @Override
  protected Void visitComparisonExpression(ComparisonExpression node, Void context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);
    return context;
  }

  @Override
  protected Void visitIsNullPredicate(IsNullPredicate node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitFunctionCall(FunctionCall node, Void context) {
    node.getArguments().forEach(arg -> arg.accept(this, context));
    return context;
  }

  @Override
  protected Void visitLikePredicate(LikePredicate node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitBetweenPredicate(BetweenPredicate node, Void context) {
    node.getValue().accept(this, context);
    node.getMin().accept(this, context);
    node.getMax().accept(this, context);
    return context;
  }

  // TODO it can be optimized by an array of expressions
  @Override
  protected Void visitInPredicate(InPredicate node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitNotExpression(NotExpression node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);
    return context;
  }

  @Override
  protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
    node.getValue().accept(this, context);
    return context;
  }

  @Override
  protected Void visitLiteral(Literal node, Void context) {
    literalList.add(node);
    node.setLiteralIndex(++literalGlobalIndex);
    return context;
  }
}
