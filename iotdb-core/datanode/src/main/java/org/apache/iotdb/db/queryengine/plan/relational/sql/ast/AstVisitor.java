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

import javax.annotation.Nullable;

public abstract class AstVisitor<R, C> {

  public R process(Node node) {
    return process(node, null);
  }

  public R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  protected R visitNode(Node node, C context) {
    return null;
  }

  protected R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  protected R visitCurrentTime(CurrentTime node, C context) {
    return visitExpression(node, context);
  }

  protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCoalesceExpression(CoalesceExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDoubleLiteral(DoubleLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitDecimalLiteral(DecimalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  protected R visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  protected R visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  protected R visitExplainAnalyze(ExplainAnalyze node, C context) {
    return visitStatement(node, context);
  }

  protected R visitUse(Use node, C context) {
    return visitStatement(node, context);
  }

  protected R visitGenericLiteral(GenericLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitWith(With node, C context) {
    return visitNode(node, context);
  }

  protected R visitWithQuery(WithQuery node, C context) {
    return visitNode(node, context);
  }

  protected R visitSelect(Select node, C context) {
    return visitNode(node, context);
  }

  protected R visitRelation(Relation node, C context) {
    return visitNode(node, context);
  }

  protected R visitQueryBody(QueryBody node, C context) {
    return visitRelation(node, context);
  }

  protected R visitOrderBy(OrderBy node, C context) {
    return visitNode(node, context);
  }

  protected R visitOffset(Offset node, C context) {
    return visitNode(node, context);
  }

  protected R visitLimit(Limit node, C context) {
    return visitNode(node, context);
  }

  protected R visitAllRows(AllRows node, C context) {
    return visitExpression(node, context);
  }

  protected R visitQuerySpecification(QuerySpecification node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitSetOperation(SetOperation node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitUnion(Union node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitIntersect(Intersect node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitExcept(Except node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  protected R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitBinaryLiteral(BinaryLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIdentifier(Identifier node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitTrim(Trim node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIfExpression(IfExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullIfExpression(NullIfExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitSingleColumn(SingleColumn node, C context) {
    return visitSelectItem(node, context);
  }

  protected R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  protected R visitLikePredicate(LikePredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitParameter(Parameter node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLogicalExpression(LogicalExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSubqueryExpression(SubqueryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSortItem(SortItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitTable(Table node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitValues(Values node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitRow(Row node, C context) {
    return visitExpression(node, context);
  }

  protected R visitTableSubquery(TableSubquery node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitAliasedRelation(AliasedRelation node, C context) {
    return visitRelation(node, context);
  }

  protected R visitJoin(Join node, C context) {
    return visitRelation(node, context);
  }

  protected R visitExists(ExistsPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFieldReference(FieldReference node, C context) {
    return visitExpression(node, context);
  }

  protected R visitColumnDefinition(ColumnDefinition node, C context) {
    return visitNode(node, context);
  }

  protected R visitCreateDB(CreateDB node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropDB(DropDB node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowDB(ShowDB node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTable(CreateTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitProperty(Property node, C context) {
    return visitNode(node, context);
  }

  protected R visitDropTable(DropTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowTables(ShowTables node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowCluster(ShowCluster node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowRegions(ShowRegions node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowDataNodes(ShowDataNodes node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowConfigNodes(ShowConfigNodes node, C context) {
    return visitStatement(node, context);
  }

  protected R visitRenameTable(RenameTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDescribeTable(DescribeTable node, C context) {
    return visitStatement(node, context);
  }

  protected R visitSetProperties(SetProperties node, C context) {
    return visitStatement(node, context);
  }

  protected R visitRenameColumn(RenameColumn node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropColumn(DropColumn node, C context) {
    return visitStatement(node, context);
  }

  protected R visitAddColumn(AddColumn node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateIndex(CreateIndex node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropIndex(DropIndex node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowIndex(ShowIndex node, C context) {
    return visitStatement(node, context);
  }

  protected R visitInsert(Insert node, C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertTablet(InsertTablet node, C context) {
    return visitStatement(node, context);
  }

  protected R visitFlush(Flush node, C context) {
    return visitStatement(node, context);
  }

  protected R visitSetConfiguration(SetConfiguration node, C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertRow(InsertRow node, C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertRows(InsertRows node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDelete(Delete node, C context) {
    return visitStatement(node, context);
  }

  protected R visitUpdate(Update node, C context) {
    return visitStatement(node, context);
  }

  protected R visitUpdateAssignment(UpdateAssignment node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupBy(GroupBy node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingElement(GroupingElement node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingSets(GroupingSets node, C context) {
    return visitGroupingElement(node, context);
  }

  protected R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    return visitGroupingElement(node, context);
  }

  protected R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }

  protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCurrentDatabase(CurrentDatabase node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCurrentUser(CurrentUser node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDataType(DataType node, C context) {
    return visitExpression(node, context);
  }

  protected R visitGenericDataType(GenericDataType node, C context) {
    return visitDataType(node, context);
  }

  protected R visitDataTypeParameter(DataTypeParameter node, C context) {
    return visitNode(node, context);
  }

  protected R visitNumericTypeParameter(NumericParameter node, C context) {
    return visitDataTypeParameter(node, context);
  }

  protected R visitTypeParameter(TypeParameter node, C context) {
    return visitDataTypeParameter(node, context);
  }

  protected R visitShowFunctions(ShowFunctions node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateFunction(CreateFunction node, C context) {
    return visitStatement(node, context);
  }

  protected R visitDropFunction(DropFunction node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateDevice(CreateOrUpdateDevice node, C context) {
    return visitStatement(node, context);
  }

  protected R visitFetchDevice(FetchDevice node, C context) {
    return visitStatement(node, context);
  }

  protected R visitShowDevice(ShowDevice node, C context) {
    return visitStatement(node, context);
  }

  protected R visitCountDevice(CountDevice node, C context) {
    return visitStatement(node, context);
  }
}
