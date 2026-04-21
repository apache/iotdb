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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExcludedPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingElement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternAlternation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternConcatenation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternPermutation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternVariable;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuantifiedPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SetOperation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WithQuery;

public abstract class DefaultTraversalVisitor<C> implements AstVisitor<Void, C> {
  @Override
  public Void visitExtract(Extract node, C context) {
    process(node.getExpression(), context);
    return null;
  }

  @Override
  public Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public Void visitBetweenPredicate(BetweenPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);

    return null;
  }

  @Override
  public Void visitCoalesceExpression(CoalesceExpression node, C context) {
    for (Expression operand : node.getOperands()) {
      process(operand, context);
    }

    return null;
  }

  @Override
  public Void visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public Void visitQuery(Query node, C context) {
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
  public Void visitExplain(Explain node, C context) {
    process(node.getStatement(), context);

    return null;
  }

  @Override
  public Void visitCopyTo(CopyTo node, C context) {
    process(node.getQueryStatement(), context);
    return null;
  }

  @Override
  public Void visitExplainAnalyze(ExplainAnalyze node, C context) {
    process(node.getStatement(), context);
    return null;
  }

  @Override
  public Void visitWith(With node, C context) {
    for (WithQuery query : node.getQueries()) {
      process(query, context);
    }

    return null;
  }

  @Override
  public Void visitWithQuery(WithQuery node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  public Void visitSelect(Select node, C context) {
    for (SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  public Void visitFill(Fill node, C context) {
    node.getFillValue().ifPresent(this::process);
    return null;
  }

  @Override
  public Void visitOrderBy(OrderBy node, C context) {
    for (SortItem sortItem : node.getSortItems()) {
      process(sortItem, context);
    }
    return null;
  }

  @Override
  public Void visitOffset(Offset node, C context) {
    process(node.getRowCount());

    return null;
  }

  @Override
  public Void visitLimit(Limit node, C context) {
    process(node.getRowCount());

    return null;
  }

  @Override
  public Void visitQuerySpecification(QuerySpecification node, C context) {
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
  public Void visitSetOperation(SetOperation node, C context) {
    for (Relation relation : node.getRelations()) {
      process(relation, context);
    }
    return null;
  }

  @Override
  public Void visitWhenClause(WhenClause node, C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);

    return null;
  }

  @Override
  public Void visitInPredicate(InPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);

    return null;
  }

  @Override
  public Void visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      process(argument, context);
    }

    return null;
  }

  @Override
  public Void visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    process(node.getOperand(), context);
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }

    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitInListExpression(InListExpression node, C context) {
    for (Expression value : node.getValues()) {
      process(value, context);
    }

    return null;
  }

  @Override
  public Void visitDereferenceExpression(DereferenceExpression node, C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  public Void visitTrim(Trim node, C context) {
    process(node.getTrimSource(), context);
    node.getTrimCharacter().ifPresent(trimChar -> process(trimChar, context));

    return null;
  }

  @Override
  public Void visitIfExpression(IfExpression node, C context) {
    process(node.getCondition(), context);
    process(node.getTrueValue(), context);
    if (node.getFalseValue().isPresent()) {
      process(node.getFalseValue().get(), context);
    }

    return null;
  }

  @Override
  public Void visitNullIfExpression(NullIfExpression node, C context) {
    process(node.getFirst(), context);
    process(node.getSecond(), context);

    return null;
  }

  @Override
  public Void visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitNotExpression(NotExpression node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitSingleColumn(SingleColumn node, C context) {
    process(node.getExpression(), context);

    return null;
  }

  @Override
  public Void visitAllColumns(AllColumns node, C context) {
    node.getTarget().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitLikePredicate(LikePredicate node, C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);
    node.getEscape().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitIsNullPredicate(IsNullPredicate node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitLogicalExpression(LogicalExpression node, C context) {
    for (Node child : node.getTerms()) {
      process(child, context);
    }

    return null;
  }

  @Override
  public Void visitSubqueryExpression(SubqueryExpression node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  public Void visitSortItem(SortItem node, C context) {
    process(node.getSortKey(), context);
    return null;
  }

  @Override
  public Void visitValues(Values node, C context) {
    for (Expression row : node.getRows()) {
      process(row, context);
    }
    return null;
  }

  @Override
  public Void visitRow(Row node, C context) {
    for (Expression expression : node.getItems()) {
      process(expression, context);
    }
    return null;
  }

  @Override
  public Void visitTableSubquery(TableSubquery node, C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  public Void visitAliasedRelation(AliasedRelation node, C context) {
    process(node.getRelation(), context);
    return null;
  }

  @Override
  public Void visitJoin(Join node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    node.getCriteria()
        .filter(JoinOn.class::isInstance)
        .ifPresent(
            criteria -> {
              Expression expression = ((JoinOn) criteria).getExpression();
              if (expression != null) {
                process(expression, context);
              }
            });

    return null;
  }

  @Override
  public Void visitExists(ExistsPredicate node, C context) {
    process(node.getSubquery(), context);

    return null;
  }

  @Override
  public Void visitCast(Cast node, C context) {
    process(node.getExpression(), context);
    return null;
  }

  @Override
  public Void visitCreateDB(CreateDB node, C context) {
    for (Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  public Void visitAlterDB(AlterDB node, C context) {
    for (Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  public Void visitCreateTable(final CreateTable node, final C context) {
    for (final Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  public Void visitCreateView(final CreateView node, final C context) {
    for (final Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  public Void visitProperty(Property node, C context) {
    process(node.getName(), context);
    if (!node.isSetToDefault()) {
      process(node.getNonDefaultValue(), context);
    }
    return null;
  }

  @Override
  public Void visitSetProperties(final SetProperties node, final C context) {
    for (final Property property : node.getProperties()) {
      process(property, context);
    }

    return null;
  }

  @Override
  public Void visitAddColumn(final AddColumn node, final C context) {
    process(node.getColumn(), context);

    return null;
  }

  @Override
  public Void visitInsert(Insert node, C context) {
    process(node.getQuery(), context);

    return null;
  }

  @Override
  public Void visitDelete(Delete node, C context) {
    process(node.getTable(), context);
    node.getWhere().ifPresent(where -> process(where, context));

    return null;
  }

  @Override
  public Void visitUpdate(Update node, C context) {
    process(node.getTable(), context);
    node.getAssignments().forEach(value -> process(value, context));
    node.getWhere().ifPresent(where -> process(where, context));

    return null;
  }

  @Override
  public Void visitUpdateAssignment(UpdateAssignment node, C context) {
    process(node.getName(), context);
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitGroupBy(GroupBy node, C context) {
    for (GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, context);
    }

    return null;
  }

  @Override
  public Void visitGroupingSets(GroupingSets node, C context) {
    for (Expression expression : node.getExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  public Void visitSimpleGroupBy(SimpleGroupBy node, C context) {
    for (Expression expression : node.getExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  public Void visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    process(node.getValue(), context);
    process(node.getSubquery(), context);

    return null;
  }

  @Override
  public Void visitExcludedPattern(ExcludedPattern node, C context) {
    process(node.getPattern(), context);

    return null;
  }

  @Override
  public Void visitPatternAlternation(PatternAlternation node, C context) {
    for (RowPattern rowPattern : node.getPatterns()) {
      process(rowPattern, context);
    }

    return null;
  }

  @Override
  public Void visitPatternConcatenation(PatternConcatenation node, C context) {
    for (RowPattern rowPattern : node.getPatterns()) {
      process(rowPattern, context);
    }

    return null;
  }

  @Override
  public Void visitPatternPermutation(PatternPermutation node, C context) {
    for (RowPattern rowPattern : node.getPatterns()) {
      process(rowPattern, context);
    }

    return null;
  }

  @Override
  public Void visitPatternVariable(PatternVariable node, C context) {
    process(node.getName(), context);

    return null;
  }

  @Override
  public Void visitQuantifiedPattern(QuantifiedPattern node, C context) {
    process(node.getPattern(), context);

    return null;
  }
}
