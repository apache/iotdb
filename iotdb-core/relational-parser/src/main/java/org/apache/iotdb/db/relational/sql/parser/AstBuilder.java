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

package org.apache.iotdb.db.relational.sql.parser;

import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.NodeLocation;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class AstBuilder extends RelationalSqlBaseVisitor<Node> {

  private int parameterPosition;

  @Nullable private final NodeLocation baseLocation;

  AstBuilder(@Nullable NodeLocation baseLocation) {
    this.baseLocation = baseLocation;
  }

  @Override
  public Node visitSingleStatement(RelationalSqlParser.SingleStatementContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Node visitUseDatabaseStatement(RelationalSqlParser.UseDatabaseStatementContext ctx) {
    return super.visitUseDatabaseStatement(ctx);
  }

  @Override
  public Node visitShowDatabasesStatement(RelationalSqlParser.ShowDatabasesStatementContext ctx) {
    return super.visitShowDatabasesStatement(ctx);
  }

  @Override
  public Node visitCreateDbStatement(RelationalSqlParser.CreateDbStatementContext ctx) {
    return super.visitCreateDbStatement(ctx);
  }

  @Override
  public Node visitDropDbStatement(RelationalSqlParser.DropDbStatementContext ctx) {
    return super.visitDropDbStatement(ctx);
  }

  @Override
  public Node visitCharsetDesc(RelationalSqlParser.CharsetDescContext ctx) {
    return super.visitCharsetDesc(ctx);
  }

  @Override
  public Node visitCreateTableStatement(RelationalSqlParser.CreateTableStatementContext ctx) {
    return super.visitCreateTableStatement(ctx);
  }

  @Override
  public Node visitColumnDefinition(RelationalSqlParser.ColumnDefinitionContext ctx) {
    return super.visitColumnDefinition(ctx);
  }

  @Override
  public Node visitColumnCategory(RelationalSqlParser.ColumnCategoryContext ctx) {
    return super.visitColumnCategory(ctx);
  }

  @Override
  public Node visitCharsetName(RelationalSqlParser.CharsetNameContext ctx) {
    return super.visitCharsetName(ctx);
  }

  @Override
  public Node visitDropTableStatement(RelationalSqlParser.DropTableStatementContext ctx) {
    return super.visitDropTableStatement(ctx);
  }

  @Override
  public Node visitShowTableStatement(RelationalSqlParser.ShowTableStatementContext ctx) {
    return super.visitShowTableStatement(ctx);
  }

  @Override
  public Node visitDescTableStatement(RelationalSqlParser.DescTableStatementContext ctx) {
    return super.visitDescTableStatement(ctx);
  }

  @Override
  public Node visitRenameTable(RelationalSqlParser.RenameTableContext ctx) {
    return super.visitRenameTable(ctx);
  }

  @Override
  public Node visitAddColumn(RelationalSqlParser.AddColumnContext ctx) {
    return super.visitAddColumn(ctx);
  }

  @Override
  public Node visitRenameColumn(RelationalSqlParser.RenameColumnContext ctx) {
    return super.visitRenameColumn(ctx);
  }

  @Override
  public Node visitDropColumn(RelationalSqlParser.DropColumnContext ctx) {
    return super.visitDropColumn(ctx);
  }

  @Override
  public Node visitSetTableProperties(RelationalSqlParser.SetTablePropertiesContext ctx) {
    return super.visitSetTableProperties(ctx);
  }

  @Override
  public Node visitCreateIndexStatement(RelationalSqlParser.CreateIndexStatementContext ctx) {
    return super.visitCreateIndexStatement(ctx);
  }

  @Override
  public Node visitIdentifierList(RelationalSqlParser.IdentifierListContext ctx) {
    return super.visitIdentifierList(ctx);
  }

  @Override
  public Node visitDropIndexStatement(RelationalSqlParser.DropIndexStatementContext ctx) {
    return super.visitDropIndexStatement(ctx);
  }

  @Override
  public Node visitShowIndexStatement(RelationalSqlParser.ShowIndexStatementContext ctx) {
    return super.visitShowIndexStatement(ctx);
  }

  @Override
  public Node visitInsertStatement(RelationalSqlParser.InsertStatementContext ctx) {
    return super.visitInsertStatement(ctx);
  }

  @Override
  public Node visitDeleteStatement(RelationalSqlParser.DeleteStatementContext ctx) {
    return super.visitDeleteStatement(ctx);
  }

  @Override
  public Node visitUpdateStatement(RelationalSqlParser.UpdateStatementContext ctx) {
    return super.visitUpdateStatement(ctx);
  }

  @Override
  public Node visitCreateFunctionStatement(RelationalSqlParser.CreateFunctionStatementContext ctx) {
    return super.visitCreateFunctionStatement(ctx);
  }

  @Override
  public Node visitUriClause(RelationalSqlParser.UriClauseContext ctx) {
    return super.visitUriClause(ctx);
  }

  @Override
  public Node visitDropFunctionStatement(RelationalSqlParser.DropFunctionStatementContext ctx) {
    return super.visitDropFunctionStatement(ctx);
  }

  @Override
  public Node visitShowFunctionsStatement(RelationalSqlParser.ShowFunctionsStatementContext ctx) {
    return super.visitShowFunctionsStatement(ctx);
  }

  @Override
  public Node visitLoadTsFileStatement(RelationalSqlParser.LoadTsFileStatementContext ctx) {
    return super.visitLoadTsFileStatement(ctx);
  }

  @Override
  public Node visitShowDevicesStatement(RelationalSqlParser.ShowDevicesStatementContext ctx) {
    return super.visitShowDevicesStatement(ctx);
  }

  @Override
  public Node visitCountDevicesStatement(RelationalSqlParser.CountDevicesStatementContext ctx) {
    return super.visitCountDevicesStatement(ctx);
  }

  @Override
  public Node visitShowClusterStatement(RelationalSqlParser.ShowClusterStatementContext ctx) {
    return super.visitShowClusterStatement(ctx);
  }

  @Override
  public Node visitShowRegionsStatement(RelationalSqlParser.ShowRegionsStatementContext ctx) {
    return super.visitShowRegionsStatement(ctx);
  }

  @Override
  public Node visitShowDataNodesStatement(RelationalSqlParser.ShowDataNodesStatementContext ctx) {
    return super.visitShowDataNodesStatement(ctx);
  }

  @Override
  public Node visitShowConfigNodesStatement(
      RelationalSqlParser.ShowConfigNodesStatementContext ctx) {
    return super.visitShowConfigNodesStatement(ctx);
  }

  @Override
  public Node visitShowClusterIdStatement(RelationalSqlParser.ShowClusterIdStatementContext ctx) {
    return super.visitShowClusterIdStatement(ctx);
  }

  @Override
  public Node visitShowRegionIdStatement(RelationalSqlParser.ShowRegionIdStatementContext ctx) {
    return super.visitShowRegionIdStatement(ctx);
  }

  @Override
  public Node visitShowTimeSlotListStatement(
      RelationalSqlParser.ShowTimeSlotListStatementContext ctx) {
    return super.visitShowTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitCountTimeSlotListStatement(
      RelationalSqlParser.CountTimeSlotListStatementContext ctx) {
    return super.visitCountTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitShowSeriesSlotListStatement(
      RelationalSqlParser.ShowSeriesSlotListStatementContext ctx) {
    return super.visitShowSeriesSlotListStatement(ctx);
  }

  @Override
  public Node visitMigrateRegionStatement(RelationalSqlParser.MigrateRegionStatementContext ctx) {
    return super.visitMigrateRegionStatement(ctx);
  }

  @Override
  public Node visitShowVariablesStatement(RelationalSqlParser.ShowVariablesStatementContext ctx) {
    return super.visitShowVariablesStatement(ctx);
  }

  @Override
  public Node visitFlushStatement(RelationalSqlParser.FlushStatementContext ctx) {
    return super.visitFlushStatement(ctx);
  }

  @Override
  public Node visitClearCacheStatement(RelationalSqlParser.ClearCacheStatementContext ctx) {
    return super.visitClearCacheStatement(ctx);
  }

  @Override
  public Node visitRepairDataStatement(RelationalSqlParser.RepairDataStatementContext ctx) {
    return super.visitRepairDataStatement(ctx);
  }

  @Override
  public Node visitSetSystemStatusStatement(
      RelationalSqlParser.SetSystemStatusStatementContext ctx) {
    return super.visitSetSystemStatusStatement(ctx);
  }

  @Override
  public Node visitShowVersionStatement(RelationalSqlParser.ShowVersionStatementContext ctx) {
    return super.visitShowVersionStatement(ctx);
  }

  @Override
  public Node visitShowQueriesStatement(RelationalSqlParser.ShowQueriesStatementContext ctx) {
    return super.visitShowQueriesStatement(ctx);
  }

  @Override
  public Node visitKillQueryStatement(RelationalSqlParser.KillQueryStatementContext ctx) {
    return super.visitKillQueryStatement(ctx);
  }

  @Override
  public Node visitLoadConfigurationStatement(
      RelationalSqlParser.LoadConfigurationStatementContext ctx) {
    return super.visitLoadConfigurationStatement(ctx);
  }

  @Override
  public Node visitLocalOrClusterMode(RelationalSqlParser.LocalOrClusterModeContext ctx) {
    return super.visitLocalOrClusterMode(ctx);
  }

  @Override
  public Node visitStatementDefault(RelationalSqlParser.StatementDefaultContext ctx) {
    return super.visitStatementDefault(ctx);
  }

  @Override
  public Node visitExplain(RelationalSqlParser.ExplainContext ctx) {
    return super.visitExplain(ctx);
  }

  @Override
  public Node visitExplainAnalyze(RelationalSqlParser.ExplainAnalyzeContext ctx) {
    return super.visitExplainAnalyze(ctx);
  }

  @Override
  public Node visitQuery(RelationalSqlParser.QueryContext ctx) {
    return super.visitQuery(ctx);
  }

  @Override
  public Node visitWith(RelationalSqlParser.WithContext ctx) {
    return super.visitWith(ctx);
  }

  @Override
  public Node visitProperties(RelationalSqlParser.PropertiesContext ctx) {
    return super.visitProperties(ctx);
  }

  @Override
  public Node visitPropertyAssignments(RelationalSqlParser.PropertyAssignmentsContext ctx) {
    return super.visitPropertyAssignments(ctx);
  }

  @Override
  public Node visitProperty(RelationalSqlParser.PropertyContext ctx) {
    return super.visitProperty(ctx);
  }

  @Override
  public Node visitDefaultPropertyValue(RelationalSqlParser.DefaultPropertyValueContext ctx) {
    return super.visitDefaultPropertyValue(ctx);
  }

  @Override
  public Node visitNonDefaultPropertyValue(RelationalSqlParser.NonDefaultPropertyValueContext ctx) {
    return super.visitNonDefaultPropertyValue(ctx);
  }

  @Override
  public Node visitQueryNoWith(RelationalSqlParser.QueryNoWithContext ctx) {
    return super.visitQueryNoWith(ctx);
  }

  @Override
  public Node visitLimitRowCount(RelationalSqlParser.LimitRowCountContext ctx) {
    return super.visitLimitRowCount(ctx);
  }

  @Override
  public Node visitRowCount(RelationalSqlParser.RowCountContext ctx) {
    return super.visitRowCount(ctx);
  }

  @Override
  public Node visitQueryTermDefault(RelationalSqlParser.QueryTermDefaultContext ctx) {
    return super.visitQueryTermDefault(ctx);
  }

  @Override
  public Node visitSetOperation(RelationalSqlParser.SetOperationContext ctx) {
    return super.visitSetOperation(ctx);
  }

  @Override
  public Node visitQueryPrimaryDefault(RelationalSqlParser.QueryPrimaryDefaultContext ctx) {
    return super.visitQueryPrimaryDefault(ctx);
  }

  @Override
  public Node visitTable(RelationalSqlParser.TableContext ctx) {
    return super.visitTable(ctx);
  }

  @Override
  public Node visitInlineTable(RelationalSqlParser.InlineTableContext ctx) {
    return super.visitInlineTable(ctx);
  }

  @Override
  public Node visitSubquery(RelationalSqlParser.SubqueryContext ctx) {
    return super.visitSubquery(ctx);
  }

  @Override
  public Node visitSortItem(RelationalSqlParser.SortItemContext ctx) {
    return super.visitSortItem(ctx);
  }

  @Override
  public Node visitQuerySpecification(RelationalSqlParser.QuerySpecificationContext ctx) {
    return super.visitQuerySpecification(ctx);
  }

  @Override
  public Node visitGroupBy(RelationalSqlParser.GroupByContext ctx) {
    return super.visitGroupBy(ctx);
  }

  @Override
  public Node visitTimenGrouping(RelationalSqlParser.TimenGroupingContext ctx) {
    return super.visitTimenGrouping(ctx);
  }

  @Override
  public Node visitVariationGrouping(RelationalSqlParser.VariationGroupingContext ctx) {
    return super.visitVariationGrouping(ctx);
  }

  @Override
  public Node visitConditionGrouping(RelationalSqlParser.ConditionGroupingContext ctx) {
    return super.visitConditionGrouping(ctx);
  }

  @Override
  public Node visitSessionGrouping(RelationalSqlParser.SessionGroupingContext ctx) {
    return super.visitSessionGrouping(ctx);
  }

  @Override
  public Node visitCountGrouping(RelationalSqlParser.CountGroupingContext ctx) {
    return super.visitCountGrouping(ctx);
  }

  @Override
  public Node visitSingleGroupingSet(RelationalSqlParser.SingleGroupingSetContext ctx) {
    return super.visitSingleGroupingSet(ctx);
  }

  @Override
  public Node visitRollup(RelationalSqlParser.RollupContext ctx) {
    return super.visitRollup(ctx);
  }

  @Override
  public Node visitCube(RelationalSqlParser.CubeContext ctx) {
    return super.visitCube(ctx);
  }

  @Override
  public Node visitMultipleGroupingSets(RelationalSqlParser.MultipleGroupingSetsContext ctx) {
    return super.visitMultipleGroupingSets(ctx);
  }

  @Override
  public Node visitLeftClosedRightOpen(RelationalSqlParser.LeftClosedRightOpenContext ctx) {
    return super.visitLeftClosedRightOpen(ctx);
  }

  @Override
  public Node visitLeftOpenRightClosed(RelationalSqlParser.LeftOpenRightClosedContext ctx) {
    return super.visitLeftOpenRightClosed(ctx);
  }

  @Override
  public Node visitTimeValue(RelationalSqlParser.TimeValueContext ctx) {
    return super.visitTimeValue(ctx);
  }

  @Override
  public Node visitDateExpression(RelationalSqlParser.DateExpressionContext ctx) {
    return super.visitDateExpression(ctx);
  }

  @Override
  public Node visitDatetimeLiteral(RelationalSqlParser.DatetimeLiteralContext ctx) {
    return super.visitDatetimeLiteral(ctx);
  }

  @Override
  public Node visitKeepExpression(RelationalSqlParser.KeepExpressionContext ctx) {
    return super.visitKeepExpression(ctx);
  }

  @Override
  public Node visitGroupingSet(RelationalSqlParser.GroupingSetContext ctx) {
    return super.visitGroupingSet(ctx);
  }

  @Override
  public Node visitNamedQuery(RelationalSqlParser.NamedQueryContext ctx) {
    return super.visitNamedQuery(ctx);
  }

  @Override
  public Node visitSetQuantifier(RelationalSqlParser.SetQuantifierContext ctx) {
    return super.visitSetQuantifier(ctx);
  }

  @Override
  public Node visitSelectSingle(RelationalSqlParser.SelectSingleContext ctx) {
    return super.visitSelectSingle(ctx);
  }

  @Override
  public Node visitSelectAll(RelationalSqlParser.SelectAllContext ctx) {
    return super.visitSelectAll(ctx);
  }

  @Override
  public Node visitRelationDefault(RelationalSqlParser.RelationDefaultContext ctx) {
    return super.visitRelationDefault(ctx);
  }

  @Override
  public Node visitJoinRelation(RelationalSqlParser.JoinRelationContext ctx) {
    return super.visitJoinRelation(ctx);
  }

  @Override
  public Node visitJoinType(RelationalSqlParser.JoinTypeContext ctx) {
    return super.visitJoinType(ctx);
  }

  @Override
  public Node visitJoinCriteria(RelationalSqlParser.JoinCriteriaContext ctx) {
    return super.visitJoinCriteria(ctx);
  }

  @Override
  public Node visitAliasedRelation(RelationalSqlParser.AliasedRelationContext ctx) {
    return super.visitAliasedRelation(ctx);
  }

  @Override
  public Node visitColumnAliases(RelationalSqlParser.ColumnAliasesContext ctx) {
    return super.visitColumnAliases(ctx);
  }

  @Override
  public Node visitTableName(RelationalSqlParser.TableNameContext ctx) {
    return super.visitTableName(ctx);
  }

  @Override
  public Node visitSubqueryRelation(RelationalSqlParser.SubqueryRelationContext ctx) {
    return super.visitSubqueryRelation(ctx);
  }

  @Override
  public Node visitParenthesizedRelation(RelationalSqlParser.ParenthesizedRelationContext ctx) {
    return super.visitParenthesizedRelation(ctx);
  }

  @Override
  public Node visitExpression(RelationalSqlParser.ExpressionContext ctx) {
    return super.visitExpression(ctx);
  }

  @Override
  public Node visitLogicalNot(RelationalSqlParser.LogicalNotContext ctx) {
    return super.visitLogicalNot(ctx);
  }

  @Override
  public Node visitPredicated(RelationalSqlParser.PredicatedContext ctx) {
    return super.visitPredicated(ctx);
  }

  @Override
  public Node visitOr(RelationalSqlParser.OrContext ctx) {
    return super.visitOr(ctx);
  }

  @Override
  public Node visitAnd(RelationalSqlParser.AndContext ctx) {
    return super.visitAnd(ctx);
  }

  @Override
  public Node visitComparison(RelationalSqlParser.ComparisonContext ctx) {
    return super.visitComparison(ctx);
  }

  @Override
  public Node visitQuantifiedComparison(RelationalSqlParser.QuantifiedComparisonContext ctx) {
    return super.visitQuantifiedComparison(ctx);
  }

  @Override
  public Node visitBetween(RelationalSqlParser.BetweenContext ctx) {
    return super.visitBetween(ctx);
  }

  @Override
  public Node visitInList(RelationalSqlParser.InListContext ctx) {
    return super.visitInList(ctx);
  }

  @Override
  public Node visitInSubquery(RelationalSqlParser.InSubqueryContext ctx) {
    return super.visitInSubquery(ctx);
  }

  @Override
  public Node visitLike(RelationalSqlParser.LikeContext ctx) {
    return super.visitLike(ctx);
  }

  @Override
  public Node visitNullPredicate(RelationalSqlParser.NullPredicateContext ctx) {
    return super.visitNullPredicate(ctx);
  }

  @Override
  public Node visitDistinctFrom(RelationalSqlParser.DistinctFromContext ctx) {
    return super.visitDistinctFrom(ctx);
  }

  @Override
  public Node visitValueExpressionDefault(RelationalSqlParser.ValueExpressionDefaultContext ctx) {
    return super.visitValueExpressionDefault(ctx);
  }

  @Override
  public Node visitConcatenation(RelationalSqlParser.ConcatenationContext ctx) {
    return super.visitConcatenation(ctx);
  }

  @Override
  public Node visitArithmeticBinary(RelationalSqlParser.ArithmeticBinaryContext ctx) {
    return super.visitArithmeticBinary(ctx);
  }

  @Override
  public Node visitArithmeticUnary(RelationalSqlParser.ArithmeticUnaryContext ctx) {
    return super.visitArithmeticUnary(ctx);
  }

  @Override
  public Node visitDereference(RelationalSqlParser.DereferenceContext ctx) {
    return super.visitDereference(ctx);
  }

  @Override
  public Node visitSimpleCase(RelationalSqlParser.SimpleCaseContext ctx) {
    return super.visitSimpleCase(ctx);
  }

  @Override
  public Node visitColumnReference(RelationalSqlParser.ColumnReferenceContext ctx) {
    return super.visitColumnReference(ctx);
  }

  @Override
  public Node visitSpecialDateTimeFunction(RelationalSqlParser.SpecialDateTimeFunctionContext ctx) {
    return super.visitSpecialDateTimeFunction(ctx);
  }

  @Override
  public Node visitSubqueryExpression(RelationalSqlParser.SubqueryExpressionContext ctx) {
    return super.visitSubqueryExpression(ctx);
  }

  @Override
  public Node visitCurrentDatabase(RelationalSqlParser.CurrentDatabaseContext ctx) {
    return super.visitCurrentDatabase(ctx);
  }

  @Override
  public Node visitSubstring(RelationalSqlParser.SubstringContext ctx) {
    return super.visitSubstring(ctx);
  }

  @Override
  public Node visitLiteral(RelationalSqlParser.LiteralContext ctx) {
    return super.visitLiteral(ctx);
  }

  @Override
  public Node visitCast(RelationalSqlParser.CastContext ctx) {
    return super.visitCast(ctx);
  }

  @Override
  public Node visitCurrentUser(RelationalSqlParser.CurrentUserContext ctx) {
    return super.visitCurrentUser(ctx);
  }

  @Override
  public Node visitParenthesizedExpression(RelationalSqlParser.ParenthesizedExpressionContext ctx) {
    return super.visitParenthesizedExpression(ctx);
  }

  @Override
  public Node visitTrim(RelationalSqlParser.TrimContext ctx) {
    return super.visitTrim(ctx);
  }

  @Override
  public Node visitFunctionCall(RelationalSqlParser.FunctionCallContext ctx) {
    return super.visitFunctionCall(ctx);
  }

  @Override
  public Node visitExists(RelationalSqlParser.ExistsContext ctx) {
    return super.visitExists(ctx);
  }

  @Override
  public Node visitSearchedCase(RelationalSqlParser.SearchedCaseContext ctx) {
    return super.visitSearchedCase(ctx);
  }

  @Override
  public Node visitNullLiteral(RelationalSqlParser.NullLiteralContext ctx) {
    return super.visitNullLiteral(ctx);
  }

  @Override
  public Node visitNumericLiteral(RelationalSqlParser.NumericLiteralContext ctx) {
    return super.visitNumericLiteral(ctx);
  }

  @Override
  public Node visitBooleanLiteral(RelationalSqlParser.BooleanLiteralContext ctx) {
    return super.visitBooleanLiteral(ctx);
  }

  @Override
  public Node visitStringLiteral(RelationalSqlParser.StringLiteralContext ctx) {
    return super.visitStringLiteral(ctx);
  }

  @Override
  public Node visitBinaryLiteral(RelationalSqlParser.BinaryLiteralContext ctx) {
    return super.visitBinaryLiteral(ctx);
  }

  @Override
  public Node visitParameter(RelationalSqlParser.ParameterContext ctx) {
    return super.visitParameter(ctx);
  }

  @Override
  public Node visitTrimsSpecification(RelationalSqlParser.TrimsSpecificationContext ctx) {
    return super.visitTrimsSpecification(ctx);
  }

  @Override
  public Node visitBasicStringLiteral(RelationalSqlParser.BasicStringLiteralContext ctx) {
    return super.visitBasicStringLiteral(ctx);
  }

  @Override
  public Node visitUnicodeStringLiteral(RelationalSqlParser.UnicodeStringLiteralContext ctx) {
    return super.visitUnicodeStringLiteral(ctx);
  }

  @Override
  public Node visitIdentifierOrString(RelationalSqlParser.IdentifierOrStringContext ctx) {
    return super.visitIdentifierOrString(ctx);
  }

  @Override
  public Node visitTimeZoneInterval(RelationalSqlParser.TimeZoneIntervalContext ctx) {
    return super.visitTimeZoneInterval(ctx);
  }

  @Override
  public Node visitTimeZoneString(RelationalSqlParser.TimeZoneStringContext ctx) {
    return super.visitTimeZoneString(ctx);
  }

  @Override
  public Node visitComparisonOperator(RelationalSqlParser.ComparisonOperatorContext ctx) {
    return super.visitComparisonOperator(ctx);
  }

  @Override
  public Node visitComparisonQuantifier(RelationalSqlParser.ComparisonQuantifierContext ctx) {
    return super.visitComparisonQuantifier(ctx);
  }

  @Override
  public Node visitBooleanValue(RelationalSqlParser.BooleanValueContext ctx) {
    return super.visitBooleanValue(ctx);
  }

  @Override
  public Node visitInterval(RelationalSqlParser.IntervalContext ctx) {
    return super.visitInterval(ctx);
  }

  @Override
  public Node visitIntervalField(RelationalSqlParser.IntervalFieldContext ctx) {
    return super.visitIntervalField(ctx);
  }

  @Override
  public Node visitTimeDuration(RelationalSqlParser.TimeDurationContext ctx) {
    return super.visitTimeDuration(ctx);
  }

  @Override
  public Node visitGenericType(RelationalSqlParser.GenericTypeContext ctx) {
    return super.visitGenericType(ctx);
  }

  @Override
  public Node visitTypeParameter(RelationalSqlParser.TypeParameterContext ctx) {
    return super.visitTypeParameter(ctx);
  }

  @Override
  public Node visitWhenClause(RelationalSqlParser.WhenClauseContext ctx) {
    return super.visitWhenClause(ctx);
  }

  @Override
  public Node visitQuantifiedPrimary(RelationalSqlParser.QuantifiedPrimaryContext ctx) {
    return super.visitQuantifiedPrimary(ctx);
  }

  @Override
  public Node visitPatternConcatenation(RelationalSqlParser.PatternConcatenationContext ctx) {
    return super.visitPatternConcatenation(ctx);
  }

  @Override
  public Node visitPatternAlternation(RelationalSqlParser.PatternAlternationContext ctx) {
    return super.visitPatternAlternation(ctx);
  }

  @Override
  public Node visitPatternVariable(RelationalSqlParser.PatternVariableContext ctx) {
    return super.visitPatternVariable(ctx);
  }

  @Override
  public Node visitEmptyPattern(RelationalSqlParser.EmptyPatternContext ctx) {
    return super.visitEmptyPattern(ctx);
  }

  @Override
  public Node visitPatternPermutation(RelationalSqlParser.PatternPermutationContext ctx) {
    return super.visitPatternPermutation(ctx);
  }

  @Override
  public Node visitGroupedPattern(RelationalSqlParser.GroupedPatternContext ctx) {
    return super.visitGroupedPattern(ctx);
  }

  @Override
  public Node visitPartitionStartAnchor(RelationalSqlParser.PartitionStartAnchorContext ctx) {
    return super.visitPartitionStartAnchor(ctx);
  }

  @Override
  public Node visitPartitionEndAnchor(RelationalSqlParser.PartitionEndAnchorContext ctx) {
    return super.visitPartitionEndAnchor(ctx);
  }

  @Override
  public Node visitExcludedPattern(RelationalSqlParser.ExcludedPatternContext ctx) {
    return super.visitExcludedPattern(ctx);
  }

  @Override
  public Node visitZeroOrMoreQuantifier(RelationalSqlParser.ZeroOrMoreQuantifierContext ctx) {
    return super.visitZeroOrMoreQuantifier(ctx);
  }

  @Override
  public Node visitOneOrMoreQuantifier(RelationalSqlParser.OneOrMoreQuantifierContext ctx) {
    return super.visitOneOrMoreQuantifier(ctx);
  }

  @Override
  public Node visitZeroOrOneQuantifier(RelationalSqlParser.ZeroOrOneQuantifierContext ctx) {
    return super.visitZeroOrOneQuantifier(ctx);
  }

  @Override
  public Node visitRangeQuantifier(RelationalSqlParser.RangeQuantifierContext ctx) {
    return super.visitRangeQuantifier(ctx);
  }

  @Override
  public Node visitUpdateAssignment(RelationalSqlParser.UpdateAssignmentContext ctx) {
    return super.visitUpdateAssignment(ctx);
  }

  @Override
  public Node visitReturnStatement(RelationalSqlParser.ReturnStatementContext ctx) {
    return super.visitReturnStatement(ctx);
  }

  @Override
  public Node visitAssignmentStatement(RelationalSqlParser.AssignmentStatementContext ctx) {
    return super.visitAssignmentStatement(ctx);
  }

  @Override
  public Node visitSimpleCaseStatement(RelationalSqlParser.SimpleCaseStatementContext ctx) {
    return super.visitSimpleCaseStatement(ctx);
  }

  @Override
  public Node visitSearchedCaseStatement(RelationalSqlParser.SearchedCaseStatementContext ctx) {
    return super.visitSearchedCaseStatement(ctx);
  }

  @Override
  public Node visitIfStatement(RelationalSqlParser.IfStatementContext ctx) {
    return super.visitIfStatement(ctx);
  }

  @Override
  public Node visitIterateStatement(RelationalSqlParser.IterateStatementContext ctx) {
    return super.visitIterateStatement(ctx);
  }

  @Override
  public Node visitLeaveStatement(RelationalSqlParser.LeaveStatementContext ctx) {
    return super.visitLeaveStatement(ctx);
  }

  @Override
  public Node visitCompoundStatement(RelationalSqlParser.CompoundStatementContext ctx) {
    return super.visitCompoundStatement(ctx);
  }

  @Override
  public Node visitLoopStatement(RelationalSqlParser.LoopStatementContext ctx) {
    return super.visitLoopStatement(ctx);
  }

  @Override
  public Node visitWhileStatement(RelationalSqlParser.WhileStatementContext ctx) {
    return super.visitWhileStatement(ctx);
  }

  @Override
  public Node visitRepeatStatement(RelationalSqlParser.RepeatStatementContext ctx) {
    return super.visitRepeatStatement(ctx);
  }

  @Override
  public Node visitCaseStatementWhenClause(RelationalSqlParser.CaseStatementWhenClauseContext ctx) {
    return super.visitCaseStatementWhenClause(ctx);
  }

  @Override
  public Node visitElseIfClause(RelationalSqlParser.ElseIfClauseContext ctx) {
    return super.visitElseIfClause(ctx);
  }

  @Override
  public Node visitElseClause(RelationalSqlParser.ElseClauseContext ctx) {
    return super.visitElseClause(ctx);
  }

  @Override
  public Node visitVariableDeclaration(RelationalSqlParser.VariableDeclarationContext ctx) {
    return super.visitVariableDeclaration(ctx);
  }

  @Override
  public Node visitSqlStatementList(RelationalSqlParser.SqlStatementListContext ctx) {
    return super.visitSqlStatementList(ctx);
  }

  @Override
  public Node visitPrivilege(RelationalSqlParser.PrivilegeContext ctx) {
    return super.visitPrivilege(ctx);
  }

  @Override
  public Node visitQualifiedName(RelationalSqlParser.QualifiedNameContext ctx) {
    return super.visitQualifiedName(ctx);
  }

  @Override
  public Node visitSpecifiedPrincipal(RelationalSqlParser.SpecifiedPrincipalContext ctx) {
    return super.visitSpecifiedPrincipal(ctx);
  }

  @Override
  public Node visitCurrentUserGrantor(RelationalSqlParser.CurrentUserGrantorContext ctx) {
    return super.visitCurrentUserGrantor(ctx);
  }

  @Override
  public Node visitCurrentRoleGrantor(RelationalSqlParser.CurrentRoleGrantorContext ctx) {
    return super.visitCurrentRoleGrantor(ctx);
  }

  @Override
  public Node visitUnspecifiedPrincipal(RelationalSqlParser.UnspecifiedPrincipalContext ctx) {
    return super.visitUnspecifiedPrincipal(ctx);
  }

  @Override
  public Node visitUserPrincipal(RelationalSqlParser.UserPrincipalContext ctx) {
    return super.visitUserPrincipal(ctx);
  }

  @Override
  public Node visitRolePrincipal(RelationalSqlParser.RolePrincipalContext ctx) {
    return super.visitRolePrincipal(ctx);
  }

  @Override
  public Node visitRoles(RelationalSqlParser.RolesContext ctx) {
    return super.visitRoles(ctx);
  }

  @Override
  public Node visitUnquotedIdentifier(RelationalSqlParser.UnquotedIdentifierContext ctx) {
    return super.visitUnquotedIdentifier(ctx);
  }

  @Override
  public Node visitQuotedIdentifier(RelationalSqlParser.QuotedIdentifierContext ctx) {
    return super.visitQuotedIdentifier(ctx);
  }

  @Override
  public Node visitBackQuotedIdentifier(RelationalSqlParser.BackQuotedIdentifierContext ctx) {
    return super.visitBackQuotedIdentifier(ctx);
  }

  @Override
  public Node visitDigitIdentifier(RelationalSqlParser.DigitIdentifierContext ctx) {
    return super.visitDigitIdentifier(ctx);
  }

  @Override
  public Node visitDecimalLiteral(RelationalSqlParser.DecimalLiteralContext ctx) {
    return super.visitDecimalLiteral(ctx);
  }

  @Override
  public Node visitDoubleLiteral(RelationalSqlParser.DoubleLiteralContext ctx) {
    return super.visitDoubleLiteral(ctx);
  }

  @Override
  public Node visitIntegerLiteral(RelationalSqlParser.IntegerLiteralContext ctx) {
    return super.visitIntegerLiteral(ctx);
  }

  @Override
  public Node visitIdentifierUser(RelationalSqlParser.IdentifierUserContext ctx) {
    return super.visitIdentifierUser(ctx);
  }

  @Override
  public Node visitStringUser(RelationalSqlParser.StringUserContext ctx) {
    return super.visitStringUser(ctx);
  }

  @Override
  public Node visitNonReserved(RelationalSqlParser.NonReservedContext ctx) {
    return super.visitNonReserved(ctx);
  }

  private NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  private NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return baseLocation != null
        ? new NodeLocation(
            token.getLine() + baseLocation.getLineNumber() - 1,
            token.getCharPositionInLine()
                + 1
                + (token.getLine() == 1 ? baseLocation.getColumnNumber() : 0))
        : new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
  }
}
