/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.exception.SemanticException;

import javax.annotation.Nullable;

public interface CommonQueryAstVisitor<R, C> extends IAstVisitor<R, C> {

  default R process(Node node) {
    return process(node, null);
  }

  default R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  default R visitNode(Node node, C context) {
    return null;
  }

  default R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  default R visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  default R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  default R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  default R visitExtract(Extract node, C context) {
    return visitExpression(node, context);
  }

  default R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  default R visitBinaryLiteral(BinaryLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitDoubleLiteral(DoubleLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitFloatLiteral(FloatLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitTimeDurationLiteral(TimeDurationLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitDecimalLiteral(DecimalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitGenericLiteral(GenericLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  default R visitCurrentDatabase(CurrentDatabase node, C context) {
    return visitExpression(node, context);
  }

  default R visitCurrentTime(CurrentTime node, C context) {
    return visitExpression(node, context);
  }

  default R visitCurrentUser(CurrentUser node, C context) {
    return visitExpression(node, context);
  }

  default R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitCoalesceExpression(CoalesceExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitLikePredicate(LikePredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitLogicalExpression(LogicalExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }

  default R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  default R visitTrim(Trim node, C context) {
    return visitExpression(node, context);
  }

  default R visitIfExpression(IfExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitNullIfExpression(NullIfExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitDataType(DataType node, C context) {
    return visitExpression(node, context);
  }

  default R visitGenericDataType(GenericDataType node, C context) {
    return visitDataType(node, context);
  }

  default R visitFrameBound(FrameBound node, C context) {
    return visitNode(node, context);
  }

  default R visitWindowFrame(WindowFrame node, C context) {
    return visitNode(node, context);
  }

  default R visitRow(Row node, C context) {
    return visitExpression(node, context);
  }

  default R visitIdentifier(Identifier node, C context) {
    return visitExpression(node, context);
  }

  default R visitDataTypeParameter(DataTypeParameter node, C context) {
    return visitNode(node, context);
  }

  default R visitNumericTypeParameter(NumericParameter node, C context) {
    return visitDataTypeParameter(node, context);
  }

  default R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  default R visitFieldReference(FieldReference node, C context) {
    return visitExpression(node, context);
  }

  default R visitGroupingElement(GroupingElement node, C context) {
    return visitNode(node, context);
  }

  default R visitGroupingSets(GroupingSets node, C context) {
    return visitGroupingElement(node, context);
  }

  default R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitExists(ExistsPredicate node, C context) {
    return visitExpression(node, context);
  }

  default R visitColumns(Columns node, C context) {
    return visitExpression(node, context);
  }

  default R visitAllRows(AllRows node, C context) {
    return visitExpression(node, context);
  }

  default R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  default R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  default R visitOrderBy(OrderBy node, C context) {
    return visitNode(node, context);
  }

  default R visitParameter(Parameter node, C context) {
    return visitExpression(node, context);
  }

  default R visitGroupBy(GroupBy node, C context) {
    return visitNode(node, context);
  }

  default R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    return visitGroupingElement(node, context);
  }

  default R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  default R visitSubqueryExpression(SubqueryExpression node, C context) {
    throw new SemanticException("Only TableSubquery is supported now");
  }

  default R visitSortItem(SortItem node, C context) {
    return visitNode(node, context);
  }

  default R visitWindowReference(WindowReference node, C context) {
    return visitNode(node, context);
  }

  default R visitWindowSpecification(WindowSpecification node, C context) {
    return visitNode(node, context);
  }

  default R visitTypeParameter(TypeParameter node, C context) {
    return visitDataTypeParameter(node, context);
  }

  default R visitValues(Values node, C context) {
    return visitQueryBody(node, context);
  }

  default R visitQueryBody(QueryBody node, C context) {
    return visitRelation(node, context);
  }

  default R visitTableSubquery(TableSubquery node, C context) {
    return visitQueryBody(node, context);
  }

  default R visitRelation(Relation node, C context) {
    return visitNode(node, context);
  }

  default R visitAliasedRelation(AliasedRelation node, C context) {
    return visitRelation(node, context);
  }

  default R visitJoin(Join node, C context) {
    return visitRelation(node, context);
  }

  default R visitSingleColumn(SingleColumn node, C context) {
    return visitSelectItem(node, context);
  }

  default R visitProcessingMode(ProcessingMode node, C context) {
    return visitNode(node, context);
  }

  default R visitWithQuery(WithQuery node, C context) {
    return visitNode(node, context);
  }

  default R visitWith(With node, C context) {
    return visitNode(node, context);
  }

  default R visitFill(Fill node, C context) {
    return visitNode(node, context);
  }

  default R visitOffset(Offset node, C context) {
    return visitNode(node, context);
  }

  default R visitLimit(Limit node, C context) {
    return visitNode(node, context);
  }

  default R visitSelect(Select node, C context) {
    return visitNode(node, context);
  }

  default R visitTable(Table node, C context) {
    return visitQueryBody(node, context);
  }

  default R visitQuerySpecification(QuerySpecification node, C context) {
    return visitQueryBody(node, context);
  }

  default R visitWindowDefinition(WindowDefinition node, C context) {
    return visitNode(node, context);
  }

  default R visitSetOperation(SetOperation node, C context) {
    return visitQueryBody(node, context);
  }

  default R visitUnion(Union node, C context) {
    return visitSetOperation(node, context);
  }

  default R visitIntersect(Intersect node, C context) {
    return visitSetOperation(node, context);
  }

  default R visitExcept(Except node, C context) {
    return visitSetOperation(node, context);
  }

  default R visitTableArgument(TableFunctionTableArgument tableFunctionTableArgument, C context) {
    return visitNode(tableFunctionTableArgument, context);
  }

  default R visitTableFunctionArgument(TableFunctionArgument tableFunctionArgument, C context) {
    return visitNode(tableFunctionArgument, context);
  }

  default R visitTableFunctionInvocation(
      TableFunctionInvocation tableFunctionInvocation, C context) {
    return visitNode(tableFunctionInvocation, context);
  }

  default R visitPatternRecognitionRelation(PatternRecognitionRelation node, C context) {
    return visitRelation(node, context);
  }

  default R visitRowPattern(RowPattern node, C context) {
    return visitNode(node, context);
  }

  default R visitPatternAlternation(PatternAlternation node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitPatternConcatenation(PatternConcatenation node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitQuantifiedPattern(QuantifiedPattern node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitAnchorPattern(AnchorPattern node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitEmptyPattern(EmptyPattern node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitExcludedPattern(ExcludedPattern node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitPatternPermutation(PatternPermutation node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitPatternVariable(PatternVariable node, C context) {
    return visitRowPattern(node, context);
  }

  default R visitPatternQuantifier(PatternQuantifier node, C context) {
    return visitNode(node, context);
  }

  default R visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, C context) {
    return visitPatternQuantifier(node, context);
  }

  default R visitOneOrMoreQuantifier(OneOrMoreQuantifier node, C context) {
    return visitPatternQuantifier(node, context);
  }

  default R visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, C context) {
    return visitPatternQuantifier(node, context);
  }

  default R visitRangeQuantifier(RangeQuantifier node, C context) {
    return visitPatternQuantifier(node, context);
  }

  default R visitMeasureDefinition(MeasureDefinition node, C context) {
    return visitNode(node, context);
  }

  default R visitSkipTo(SkipTo node, C context) {
    return visitNode(node, context);
  }

  default R visitSubsetDefinition(SubsetDefinition node, C context) {
    return visitNode(node, context);
  }

  default R visitVariableDefinition(VariableDefinition node, C context) {
    return visitNode(node, context);
  }
}
