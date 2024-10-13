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

public abstract class DefaultTraversalVisitor<C> extends AstVisitor<Void, C> {

  @Override
  protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected Void visitBetweenPredicate(BetweenPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);

    return null;
  }

  @Override
  protected Void visitCoalesceExpression(CoalesceExpression node, C context) {
    for (Expression operand : node.getOperands()) {
      process(operand, context);
    }

    return null;
  }

  @Override
  protected Void visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected Void visitQuery(Query node, C context) {
    if (node.getWith().isPresent()) {
      process(node.getWith().get(), context);
    }
    process(node.getQueryBody(), context);
    if (node.getOrderBy().isPresent()) {
      process(node.getOrderBy().get(), context);
    }
    if (node.getOffset().isPresent()) {
      process(node.getOffset().get(), context);
    }
    if (node.getLimit().isPresent()) {
      process(node.getLimit().get(), context);
    }

    return null;
  }

  @Override
  protected Void visitExplain(Explain node, C context) {
    process(node.getStatement(), context);

    return null;
  }

  @Override
  protected Void visitExplainAnalyze(ExplainAnalyze node, C context) {
    process(node.getStatement(), context);
    return null;
  }

  @Override
  protected Void visitWith(With node, C context) {
    for (WithQuery query : node.getQueries()) {
      process(query, context);
    }

    return null;
  }

  @Override
  protected Void visitWithQuery(WithQuery node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected Void visitSelect(Select node, C context) {
    for (SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  protected Void visitFill(Fill node, C context) {
    node.getFillValue().ifPresent(this::process);
    return null;
  }

  @Override
  protected Void visitOrderBy(OrderBy node, C context) {
    for (SortItem sortItem : node.getSortItems()) {
      process(sortItem, context);
    }
    return null;
  }

  @Override
  protected Void visitOffset(Offset node, C context) {
    process(node.getRowCount());

    return null;
  }

  @Override
  protected Void visitLimit(Limit node, C context) {
    process(node.getRowCount());

    return null;
  }

  @Override
  protected Void visitQuerySpecification(QuerySpecification node, C context) {
    process(node.getSelect(), context);
    if (node.getFrom().isPresent()) {
      process(node.getFrom().get(), context);
    }
    if (node.getWhere().isPresent()) {
      process(node.getWhere().get(), context);
    }
    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), context);
    }
    if (node.getHaving().isPresent()) {
      process(node.getHaving().get(), context);
    }
    if (node.getOrderBy().isPresent()) {
      process(node.getOrderBy().get(), context);
    }
    if (node.getOffset().isPresent()) {
      process(node.getOffset().get(), context);
    }
    if (node.getLimit().isPresent()) {
      process(node.getLimit().get(), context);
    }
    return null;
  }

  @Override
  protected Void visitSetOperation(SetOperation node, C context) {
    for (Relation relation : node.getRelations()) {
      process(relation, context);
    }
    return null;
  }

  @Override
  protected Void visitWhenClause(WhenClause node, C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);

    return null;
  }

  @Override
  protected Void visitInPredicate(InPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);

    return null;
  }

  @Override
  protected Void visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      process(argument, context);
    }

    return null;
  }

  @Override
  protected Void visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    process(node.getOperand(), context);
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }

    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected Void visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected Void visitInListExpression(InListExpression node, C context) {
    for (Expression value : node.getValues()) {
      process(value, context);
    }

    return null;
  }

  @Override
  protected Void visitDereferenceExpression(DereferenceExpression node, C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  protected Void visitTrim(Trim node, C context) {
    process(node.getTrimSource(), context);
    node.getTrimCharacter().ifPresent(trimChar -> process(trimChar, context));

    return null;
  }

  @Override
  protected Void visitIfExpression(IfExpression node, C context) {
    process(node.getCondition(), context);
    process(node.getTrueValue(), context);
    if (node.getFalseValue().isPresent()) {
      process(node.getFalseValue().get(), context);
    }

    return null;
  }

  @Override
  protected Void visitNullIfExpression(NullIfExpression node, C context) {
    process(node.getFirst(), context);
    process(node.getSecond(), context);

    return null;
  }

  @Override
  protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  protected Void visitNotExpression(NotExpression node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  protected Void visitSingleColumn(SingleColumn node, C context) {
    process(node.getExpression(), context);

    return null;
  }

  @Override
  protected Void visitAllColumns(AllColumns node, C context) {
    node.getTarget().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected Void visitLikePredicate(LikePredicate node, C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);
    node.getEscape().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected Void visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  protected Void visitIsNullPredicate(IsNullPredicate node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  protected Void visitLogicalExpression(LogicalExpression node, C context) {
    for (Node child : node.getTerms()) {
      process(child, context);
    }

    return null;
  }

  @Override
  protected Void visitSubqueryExpression(SubqueryExpression node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected Void visitSortItem(SortItem node, C context) {
    process(node.getSortKey(), context);
    return null;
  }

  @Override
  protected Void visitValues(Values node, C context) {
    for (Expression row : node.getRows()) {
      process(row, context);
    }
    return null;
  }

  @Override
  protected Void visitRow(Row node, C context) {
    for (Expression expression : node.getItems()) {
      process(expression, context);
    }
    return null;
  }

  @Override
  protected Void visitTableSubquery(TableSubquery node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected Void visitAliasedRelation(AliasedRelation node, C context) {
    process(node.getRelation(), context);
    return null;
  }

  @Override
  protected Void visitJoin(Join node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    node.getCriteria()
        .filter(JoinOn.class::isInstance)
        .ifPresent(criteria -> process(((JoinOn) criteria).getExpression(), context));

    return null;
  }

  @Override
  protected Void visitExists(ExistsPredicate node, C context) {
    process(node.getSubquery(), context);

    return null;
  }

  @Override
  protected Void visitCast(Cast node, C context) {
    process(node.getExpression(), context);
    return null;
  }

  @Override
  protected Void visitCreateDB(CreateDB node, C context) {
    for (Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  protected Void visitCreateTable(final CreateTable node, final C context) {
    for (final Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  protected Void visitProperty(Property node, C context) {
    process(node.getName(), context);
    if (!node.isSetToDefault()) {
      process(node.getNonDefaultValue(), context);
    }
    return null;
  }

  @Override
  protected Void visitSetProperties(final SetProperties node, final C context) {
    for (final Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  protected Void visitAddColumn(final AddColumn node, final C context) {
    process(node.getColumn(), context);

    return null;
  }

  @Override
  protected Void visitInsert(Insert node, C context) {
    process(node.getQuery(), context);

    return null;
  }

  @Override
  protected Void visitDelete(Delete node, C context) {
    process(node.getTable(), context);
    node.getWhere().ifPresent(where -> process(where, context));

    return null;
  }

  @Override
  protected Void visitUpdate(Update node, C context) {
    process(node.getTable(), context);
    node.getAssignments().forEach(value -> process(value, context));
    node.getWhere().ifPresent(where -> process(where, context));

    return null;
  }

  @Override
  protected Void visitUpdateAssignment(UpdateAssignment node, C context) {
    process(node.getName(), context);
    process(node.getValue(), context);
    return null;
  }

  @Override
  protected Void visitGroupBy(GroupBy node, C context) {
    for (GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, context);
    }

    return null;
  }

  @Override
  protected Void visitGroupingSets(GroupingSets node, C context) {
    for (Expression expression : node.getExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  protected Void visitSimpleGroupBy(SimpleGroupBy node, C context) {
    for (Expression expression : node.getExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  protected Void visitQuantifiedComparisonExpression(
      QuantifiedComparisonExpression node, C context) {
    process(node.getValue(), context);
    process(node.getSubquery(), context);

    return null;
  }
}
