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

package org.apache.iotdb.db.queryengine.plan.relational.sql;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AnchorPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Columns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Deallocate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DecimalLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.EmptyPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExcludedPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Execute;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExecuteImmediate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MeasureDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OneOrMoreQuantifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternAlternation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternConcatenation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternPermutation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternVariable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Prepare;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ProcessingMode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RangeQuantifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetOperation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubsetDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionTableArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.VariableDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ViewFieldDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ZeroOrMoreQuantifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ZeroOrOneQuantifier;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;

/** Utility class for estimating memory usage of AST nodes. */
public final class AstMemoryEstimator {
  private AstMemoryEstimator() {}

  public static long estimateMemorySize(Statement statement) {
    if (statement == null) {
      return 0L;
    }
    MemoryEstimatingVisitor visitor = new MemoryEstimatingVisitor();
    visitor.process(statement, null);
    return visitor.getTotalMemorySize();
  }

  private static class MemoryEstimatingVisitor extends DefaultTraversalVisitor<Void> {
    private long totalMemorySize = 0L;

    public long getTotalMemorySize() {
      return totalMemorySize;
    }

    private void addNodeSize(Node node) {
      totalMemorySize += RamUsageEstimator.shallowSizeOfInstance(node.getClass());
    }

    private void addOptionalSize() {
      totalMemorySize += RamUsageEstimator.shallowSizeOfInstance(Optional.class);
    }

    private void addListSize(List<?> list) {
      if (list != null) {
        totalMemorySize += RamUsageEstimator.shallowSizeOf(list);
      }
    }

    private void addStringSize(String str) {
      if (str != null) {
        totalMemorySize += RamUsageEstimator.sizeOf(str);
      }
    }

    private void addNodeLocationSize(Node node) {
      if (node.getLocation().isPresent()) {
        addOptionalSize();
        totalMemorySize += RamUsageEstimator.shallowSizeOfInstance(NodeLocation.class);
      }
    }

    private long estimateQualifiedNameSize(QualifiedName name) {
      if (name == null) {
        return 0L;
      }
      long size = RamUsageEstimator.shallowSizeOfInstance(QualifiedName.class);
      addListSize(name.getOriginalParts());
      if (name.getParts() != null) {
        size += RamUsageEstimator.shallowSizeOf(name.getParts());
        for (String part : name.getParts()) {
          if (part != null) {
            size += RamUsageEstimator.sizeOf(part);
          }
        }
      }
      if (name.getSuffix() != null) {
        size += RamUsageEstimator.sizeOf(name.getSuffix());
      }
      if (name.getPrefix().isPresent()) {
        size += RamUsageEstimator.shallowSizeOfInstance(Optional.class);
        size += estimateQualifiedNameSize(name.getPrefix().get());
      }
      return size;
    }

    @Override
    protected Void visitNode(Node node, Void context) {
      addNodeSize(node);
      addNodeLocationSize(node);
      return null;
    }

    @Override
    protected Void visitStringLiteral(StringLiteral node, Void context) {
      addNodeSize(node);
      addStringSize(node.getValue());
      return super.visitStringLiteral(node, context);
    }

    @Override
    protected Void visitLongLiteral(LongLiteral node, Void context) {
      addNodeSize(node);
      addStringSize(node.getValue());
      return super.visitLongLiteral(node, context);
    }

    @Override
    protected Void visitDoubleLiteral(DoubleLiteral node, Void context) {
      addNodeSize(node);
      return super.visitDoubleLiteral(node, context);
    }

    @Override
    protected Void visitDecimalLiteral(DecimalLiteral node, Void context) {
      addNodeSize(node);
      addStringSize(node.getValue());
      return super.visitDecimalLiteral(node, context);
    }

    @Override
    protected Void visitBooleanLiteral(BooleanLiteral node, Void context) {
      addNodeSize(node);
      return super.visitBooleanLiteral(node, context);
    }

    @Override
    protected Void visitBinaryLiteral(BinaryLiteral node, Void context) {
      addNodeSize(node);
      byte[] value = node.getValue();
      if (value != null) {
        totalMemorySize += RamUsageEstimator.sizeOf(value);
      }
      return super.visitBinaryLiteral(node, context);
    }

    @Override
    protected Void visitGenericLiteral(GenericLiteral node, Void context) {
      addNodeSize(node);
      addStringSize(node.getType());
      addStringSize(node.getValue());
      return super.visitGenericLiteral(node, context);
    }

    @Override
    protected Void visitNullLiteral(NullLiteral node, Void context) {
      addNodeSize(node);
      return super.visitNullLiteral(node, context);
    }

    @Override
    protected Void visitIdentifier(Identifier node, Void context) {
      addNodeSize(node);
      addStringSize(node.getValue());
      return super.visitIdentifier(node, context);
    }

    @Override
    protected Void visitSymbolReference(SymbolReference node, Void context) {
      addNodeSize(node);
      addStringSize(node.getName());
      return super.visitSymbolReference(node, context);
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context) {
      addNodeSize(node);
      if (node.getName() != null) {
        totalMemorySize += estimateQualifiedNameSize(node.getName());
      }
      addListSize(node.getArguments());
      if (node.getWindow().isPresent()) {
        addOptionalSize();
      }
      if (node.getProcessingMode().isPresent()) {
        addOptionalSize();
      }
      if (node.getNullTreatment().isPresent()) {
        addOptionalSize();
      }
      return super.visitFunctionCall(node, context);
    }

    @Override
    protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
      addNodeSize(node);
      return super.visitArithmeticBinary(node, context);
    }

    @Override
    protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
      addNodeSize(node);
      return super.visitArithmeticUnary(node, context);
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context) {
      addNodeSize(node);
      return super.visitComparisonExpression(node, context);
    }

    @Override
    protected Void visitLogicalExpression(LogicalExpression node, Void context) {
      addNodeSize(node);
      addListSize(node.getTerms());
      return super.visitLogicalExpression(node, context);
    }

    @Override
    protected Void visitNotExpression(NotExpression node, Void context) {
      addNodeSize(node);
      return super.visitNotExpression(node, context);
    }

    @Override
    protected Void visitBetweenPredicate(BetweenPredicate node, Void context) {
      addNodeSize(node);
      return super.visitBetweenPredicate(node, context);
    }

    @Override
    protected Void visitInPredicate(InPredicate node, Void context) {
      addNodeSize(node);
      return super.visitInPredicate(node, context);
    }

    @Override
    protected Void visitInListExpression(InListExpression node, Void context) {
      addNodeSize(node);
      addListSize(node.getValues());
      return super.visitInListExpression(node, context);
    }

    @Override
    protected Void visitLikePredicate(LikePredicate node, Void context) {
      addNodeSize(node);
      if (node.getEscape().isPresent()) {
        addOptionalSize();
      }
      return super.visitLikePredicate(node, context);
    }

    @Override
    protected Void visitIsNullPredicate(IsNullPredicate node, Void context) {
      addNodeSize(node);
      return super.visitIsNullPredicate(node, context);
    }

    @Override
    protected Void visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
      addNodeSize(node);
      return super.visitIsNotNullPredicate(node, context);
    }

    @Override
    protected Void visitCoalesceExpression(CoalesceExpression node, Void context) {
      addNodeSize(node);
      addListSize(node.getOperands());
      return super.visitCoalesceExpression(node, context);
    }

    @Override
    protected Void visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
      addNodeSize(node);
      addListSize(node.getWhenClauses());
      if (node.getDefaultValue().isPresent()) {
        addOptionalSize();
      }
      return super.visitSimpleCaseExpression(node, context);
    }

    @Override
    protected Void visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
      addNodeSize(node);
      addListSize(node.getWhenClauses());
      if (node.getDefaultValue().isPresent()) {
        addOptionalSize();
      }
      return super.visitSearchedCaseExpression(node, context);
    }

    @Override
    protected Void visitWhenClause(WhenClause node, Void context) {
      addNodeSize(node);
      return super.visitWhenClause(node, context);
    }

    @Override
    protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
      addNodeSize(node);
      if (node.getField().isPresent()) {
        addOptionalSize();
      }
      return super.visitDereferenceExpression(node, context);
    }

    @Override
    protected Void visitTrim(Trim node, Void context) {
      addNodeSize(node);
      if (node.getTrimCharacter().isPresent()) {
        addOptionalSize();
      }
      return super.visitTrim(node, context);
    }

    @Override
    protected Void visitIfExpression(IfExpression node, Void context) {
      addNodeSize(node);
      if (node.getFalseValue().isPresent()) {
        addOptionalSize();
      }
      return super.visitIfExpression(node, context);
    }

    @Override
    protected Void visitNullIfExpression(NullIfExpression node, Void context) {
      addNodeSize(node);
      return super.visitNullIfExpression(node, context);
    }

    @Override
    protected Void visitExtract(Extract node, Void context) {
      addNodeSize(node);
      return super.visitExtract(node, context);
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, Void context) {
      addNodeSize(node);
      return super.visitSubqueryExpression(node, context);
    }

    @Override
    protected Void visitExists(ExistsPredicate node, Void context) {
      addNodeSize(node);
      return super.visitExists(node, context);
    }

    @Override
    protected Void visitCast(Cast node, Void context) {
      addNodeSize(node);
      return super.visitCast(node, context);
    }

    @Override
    protected Void visitFieldReference(FieldReference node, Void context) {
      addNodeSize(node);
      return super.visitFieldReference(node, context);
    }

    @Override
    protected Void visitParameter(Parameter node, Void context) {
      addNodeSize(node);
      return super.visitParameter(node, context);
    }

    @Override
    protected Void visitQuantifiedComparisonExpression(
        QuantifiedComparisonExpression node, Void context) {
      addNodeSize(node);
      return super.visitQuantifiedComparisonExpression(node, context);
    }

    @Override
    protected Void visitCurrentTime(CurrentTime node, Void context) {
      addNodeSize(node);
      if (node.getPrecision().isPresent()) {
        addOptionalSize();
      }
      return super.visitCurrentTime(node, context);
    }

    @Override
    protected Void visitCurrentDatabase(CurrentDatabase node, Void context) {
      addNodeSize(node);
      return super.visitCurrentDatabase(node, context);
    }

    @Override
    protected Void visitCurrentUser(CurrentUser node, Void context) {
      addNodeSize(node);
      return super.visitCurrentUser(node, context);
    }

    @Override
    protected Void visitRow(Row node, Void context) {
      addNodeSize(node);
      addListSize(node.getItems());
      return super.visitRow(node, context);
    }

    @Override
    protected Void visitAllRows(AllRows node, Void context) {
      addNodeSize(node);
      return super.visitAllRows(node, context);
    }

    @Override
    protected Void visitColumns(Columns node, Void context) {
      addNodeSize(node);
      addStringSize(node.getPattern());
      return super.visitColumns(node, context);
    }

    @Override
    protected Void visitGenericDataType(GenericDataType node, Void context) {
      addNodeSize(node);
      addListSize(node.getArguments());
      return super.visitGenericDataType(node, context);
    }

    @Override
    protected Void visitNumericTypeParameter(NumericParameter node, Void context) {
      addNodeSize(node);
      addStringSize(node.getValue());
      return super.visitNumericTypeParameter(node, context);
    }

    @Override
    protected Void visitTypeParameter(TypeParameter node, Void context) {
      addNodeSize(node);
      return super.visitTypeParameter(node, context);
    }

    @Override
    protected Void visitQuery(Query node, Void context) {
      addNodeSize(node);
      if (node.getWith().isPresent()) {
        addOptionalSize();
      }
      if (node.getFill().isPresent()) {
        addOptionalSize();
      }
      if (node.getOrderBy().isPresent()) {
        addOptionalSize();
      }
      if (node.getOffset().isPresent()) {
        addOptionalSize();
      }
      if (node.getLimit().isPresent()) {
        addOptionalSize();
      }
      return super.visitQuery(node, context);
    }

    @Override
    protected Void visitWith(With node, Void context) {
      addNodeSize(node);
      addListSize(node.getQueries());
      return super.visitWith(node, context);
    }

    @Override
    protected Void visitWithQuery(WithQuery node, Void context) {
      addNodeSize(node);
      if (node.getColumnNames().isPresent()) {
        addOptionalSize();
        addListSize(node.getColumnNames().get());
      }
      return super.visitWithQuery(node, context);
    }

    @Override
    protected Void visitSelect(Select node, Void context) {
      addNodeSize(node);
      addListSize(node.getSelectItems());
      return super.visitSelect(node, context);
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context) {
      addNodeSize(node);
      List<Expression> expandedExpressions = node.getExpandedExpressions();
      addListSize(expandedExpressions);
      List<String> accordingColumnNames = node.getAccordingColumnNames();
      if (accordingColumnNames != null) {
        totalMemorySize += RamUsageEstimator.shallowSizeOf(accordingColumnNames);
        for (String name : accordingColumnNames) {
          addStringSize(name);
        }
      }
      return super.visitSingleColumn(node, context);
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Void context) {
      addNodeSize(node);
      if (node.getTarget().isPresent()) {
        addOptionalSize();
      }
      addListSize(node.getAliases());
      return super.visitAllColumns(node, context);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Void context) {
      addNodeSize(node);
      addListSize(node.getWindows());
      if (node.getFrom().isPresent()) {
        addOptionalSize();
      }
      if (node.getWhere().isPresent()) {
        addOptionalSize();
      }
      if (node.getGroupBy().isPresent()) {
        addOptionalSize();
      }
      if (node.getHaving().isPresent()) {
        addOptionalSize();
      }
      if (node.getFill().isPresent()) {
        addOptionalSize();
      }
      if (node.getOrderBy().isPresent()) {
        addOptionalSize();
      }
      if (node.getOffset().isPresent()) {
        addOptionalSize();
      }
      if (node.getLimit().isPresent()) {
        addOptionalSize();
      }
      return super.visitQuerySpecification(node, context);
    }

    @Override
    protected Void visitValues(Values node, Void context) {
      addNodeSize(node);
      addListSize(node.getRows());
      return super.visitValues(node, context);
    }

    @Override
    protected Void visitOrderBy(OrderBy node, Void context) {
      addNodeSize(node);
      addListSize(node.getSortItems());
      return super.visitOrderBy(node, context);
    }

    @Override
    protected Void visitSortItem(SortItem node, Void context) {
      addNodeSize(node);
      return super.visitSortItem(node, context);
    }

    @Override
    protected Void visitGroupBy(GroupBy node, Void context) {
      addNodeSize(node);
      addListSize(node.getGroupingElements());
      return super.visitGroupBy(node, context);
    }

    @Override
    protected Void visitGroupingSets(GroupingSets node, Void context) {
      addNodeSize(node);
      addListSize(node.getExpressions());
      return super.visitGroupingSets(node, context);
    }

    @Override
    protected Void visitSimpleGroupBy(SimpleGroupBy node, Void context) {
      addNodeSize(node);
      addListSize(node.getExpressions());
      return super.visitSimpleGroupBy(node, context);
    }

    @Override
    protected Void visitFill(Fill node, Void context) {
      addNodeSize(node);
      if (node.getFillValue().isPresent()) {
        addOptionalSize();
      }
      return super.visitFill(node, context);
    }

    @Override
    protected Void visitOffset(Offset node, Void context) {
      addNodeSize(node);
      return super.visitOffset(node, context);
    }

    @Override
    protected Void visitLimit(Limit node, Void context) {
      addNodeSize(node);
      return super.visitLimit(node, context);
    }

    @Override
    protected Void visitSetOperation(SetOperation node, Void context) {
      addNodeSize(node);
      addListSize(node.getRelations());
      return super.visitSetOperation(node, context);
    }

    @Override
    protected Void visitUnion(Union node, Void context) {
      addNodeSize(node);
      addListSize(node.getRelations());
      return super.visitUnion(node, context);
    }

    @Override
    protected Void visitIntersect(Intersect node, Void context) {
      addNodeSize(node);
      addListSize(node.getRelations());
      return super.visitIntersect(node, context);
    }

    @Override
    protected Void visitExcept(Except node, Void context) {
      addNodeSize(node);
      return super.visitExcept(node, context);
    }

    @Override
    protected Void visitTable(Table node, Void context) {
      addNodeSize(node);
      if (node.getName() != null) {
        totalMemorySize += estimateQualifiedNameSize(node.getName());
      }
      return super.visitTable(node, context);
    }

    @Override
    protected Void visitTableSubquery(TableSubquery node, Void context) {
      addNodeSize(node);
      return super.visitTableSubquery(node, context);
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Void context) {
      addNodeSize(node);
      addListSize(node.getColumnNames());
      return super.visitAliasedRelation(node, context);
    }

    @Override
    protected Void visitJoin(Join node, Void context) {
      addNodeSize(node);
      if (node.getCriteria().isPresent()) {
        addOptionalSize();
        JoinCriteria criteria = node.getCriteria().get();
        totalMemorySize += RamUsageEstimator.shallowSizeOfInstance(criteria.getClass());
        if (criteria instanceof JoinUsing) {
          addListSize(((JoinUsing) criteria).getColumns());
        }
      }
      return super.visitJoin(node, context);
    }

    @Override
    public Void visitTableArgument(TableFunctionTableArgument node, Void context) {
      addNodeSize(node);
      if (node.getPartitionBy().isPresent()) {
        addOptionalSize();
        addListSize(node.getPartitionBy().get());
      }
      if (node.getOrderBy().isPresent()) {
        addOptionalSize();
      }
      return super.visitTableArgument(node, context);
    }

    @Override
    public Void visitTableFunctionArgument(TableFunctionArgument node, Void context) {
      addNodeSize(node);
      if (node.getName().isPresent()) {
        addOptionalSize();
      }
      return super.visitTableFunctionArgument(node, context);
    }

    @Override
    public Void visitTableFunctionInvocation(TableFunctionInvocation node, Void context) {
      addNodeSize(node);
      if (node.getName() != null) {
        totalMemorySize += estimateQualifiedNameSize(node.getName());
      }
      addListSize(node.getArguments());
      return super.visitTableFunctionInvocation(node, context);
    }

    @Override
    protected Void visitWindowDefinition(WindowDefinition node, Void context) {
      addNodeSize(node);
      return super.visitWindowDefinition(node, context);
    }

    @Override
    protected Void visitWindowSpecification(WindowSpecification node, Void context) {
      addNodeSize(node);
      if (node.getExistingWindowName().isPresent()) {
        addOptionalSize();
      }
      addListSize(node.getPartitionBy());
      if (node.getOrderBy().isPresent()) {
        addOptionalSize();
      }
      if (node.getFrame().isPresent()) {
        addOptionalSize();
      }
      return super.visitWindowSpecification(node, context);
    }

    @Override
    protected Void visitWindowReference(WindowReference node, Void context) {
      addNodeSize(node);
      return super.visitWindowReference(node, context);
    }

    @Override
    protected Void visitWindowFrame(WindowFrame node, Void context) {
      addNodeSize(node);
      if (node.getEnd().isPresent()) {
        addOptionalSize();
      }
      return super.visitWindowFrame(node, context);
    }

    @Override
    protected Void visitFrameBound(FrameBound node, Void context) {
      addNodeSize(node);
      if (node.getValue().isPresent()) {
        addOptionalSize();
      }
      return super.visitFrameBound(node, context);
    }

    @Override
    protected Void visitProcessingMode(ProcessingMode node, Void context) {
      addNodeSize(node);
      return super.visitProcessingMode(node, context);
    }

    @Override
    protected Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Void context) {
      addNodeSize(node);
      addListSize(node.getPartitionBy());
      if (node.getOrderBy().isPresent()) {
        addOptionalSize();
      }
      addListSize(node.getMeasures());
      if (node.getRowsPerMatch().isPresent()) {
        addOptionalSize();
      }
      if (node.getAfterMatchSkipTo().isPresent()) {
        addOptionalSize();
      }
      addListSize(node.getSubsets());
      addListSize(node.getVariableDefinitions());
      return super.visitPatternRecognitionRelation(node, context);
    }

    @Override
    protected Void visitMeasureDefinition(MeasureDefinition node, Void context) {
      addNodeSize(node);
      return super.visitMeasureDefinition(node, context);
    }

    @Override
    protected Void visitSkipTo(SkipTo node, Void context) {
      addNodeSize(node);
      if (node.getIdentifier().isPresent()) {
        addOptionalSize();
      }
      return super.visitSkipTo(node, context);
    }

    @Override
    protected Void visitSubsetDefinition(SubsetDefinition node, Void context) {
      addNodeSize(node);
      addListSize(node.getIdentifiers());
      return super.visitSubsetDefinition(node, context);
    }

    @Override
    protected Void visitVariableDefinition(VariableDefinition node, Void context) {
      addNodeSize(node);
      return super.visitVariableDefinition(node, context);
    }

    @Override
    protected Void visitPatternAlternation(PatternAlternation node, Void context) {
      addNodeSize(node);
      addListSize(node.getPatterns());
      return super.visitPatternAlternation(node, context);
    }

    @Override
    protected Void visitPatternConcatenation(PatternConcatenation node, Void context) {
      addNodeSize(node);
      addListSize(node.getPatterns());
      return super.visitPatternConcatenation(node, context);
    }

    @Override
    protected Void visitPatternPermutation(PatternPermutation node, Void context) {
      addNodeSize(node);
      addListSize(node.getPatterns());
      return super.visitPatternPermutation(node, context);
    }

    @Override
    protected Void visitQuantifiedPattern(QuantifiedPattern node, Void context) {
      addNodeSize(node);
      return super.visitQuantifiedPattern(node, context);
    }

    @Override
    protected Void visitAnchorPattern(AnchorPattern node, Void context) {
      addNodeSize(node);
      return super.visitAnchorPattern(node, context);
    }

    @Override
    protected Void visitEmptyPattern(EmptyPattern node, Void context) {
      addNodeSize(node);
      return super.visitEmptyPattern(node, context);
    }

    @Override
    protected Void visitExcludedPattern(ExcludedPattern node, Void context) {
      addNodeSize(node);
      return super.visitExcludedPattern(node, context);
    }

    @Override
    protected Void visitPatternVariable(PatternVariable node, Void context) {
      addNodeSize(node);
      return super.visitPatternVariable(node, context);
    }

    @Override
    protected Void visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, Void context) {
      addNodeSize(node);
      return super.visitZeroOrMoreQuantifier(node, context);
    }

    @Override
    protected Void visitOneOrMoreQuantifier(OneOrMoreQuantifier node, Void context) {
      addNodeSize(node);
      return super.visitOneOrMoreQuantifier(node, context);
    }

    @Override
    protected Void visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, Void context) {
      addNodeSize(node);
      return super.visitZeroOrOneQuantifier(node, context);
    }

    @Override
    protected Void visitRangeQuantifier(RangeQuantifier node, Void context) {
      addNodeSize(node);
      if (node.getAtLeast().isPresent()) {
        addOptionalSize();
      }
      if (node.getAtMost().isPresent()) {
        addOptionalSize();
      }
      return super.visitRangeQuantifier(node, context);
    }

    @Override
    protected Void visitExplain(Explain node, Void context) {
      addNodeSize(node);
      return super.visitExplain(node, context);
    }

    @Override
    protected Void visitExplainAnalyze(ExplainAnalyze node, Void context) {
      addNodeSize(node);
      return super.visitExplainAnalyze(node, context);
    }

    @Override
    protected Void visitInsert(Insert node, Void context) {
      addNodeSize(node);
      if (node.getColumns().isPresent()) {
        addOptionalSize();
        addListSize(node.getColumns().get());
      }
      return super.visitInsert(node, context);
    }

    @Override
    protected Void visitDelete(Delete node, Void context) {
      addNodeSize(node);
      if (node.getWhere().isPresent()) {
        addOptionalSize();
      }
      return super.visitDelete(node, context);
    }

    @Override
    protected Void visitUpdate(Update node, Void context) {
      addNodeSize(node);
      addListSize(node.getAssignments());
      if (node.getWhere().isPresent()) {
        addOptionalSize();
      }
      return super.visitUpdate(node, context);
    }

    @Override
    protected Void visitUpdateAssignment(UpdateAssignment node, Void context) {
      addNodeSize(node);
      return super.visitUpdateAssignment(node, context);
    }

    @Override
    protected Void visitProperty(Property node, Void context) {
      addNodeSize(node);
      return super.visitProperty(node, context);
    }

    @Override
    protected Void visitColumnDefinition(ColumnDefinition node, Void context) {
      addNodeSize(node);
      if (node.getCharsetName().isPresent()) {
        addOptionalSize();
        addStringSize(node.getCharsetName().get());
      }
      if (node.getComment() != null) {
        addStringSize(node.getComment());
      }
      return super.visitColumnDefinition(node, context);
    }

    @Override
    protected Void visitViewFieldDefinition(ViewFieldDefinition node, Void context) {
      addNodeSize(node);
      if (node.getCharsetName().isPresent()) {
        addOptionalSize();
        addStringSize(node.getCharsetName().get());
      }
      if (node.getComment() != null) {
        addStringSize(node.getComment());
      }
      return super.visitViewFieldDefinition(node, context);
    }

    @Override
    protected Void visitPrepare(Prepare node, Void context) {
      addNodeSize(node);
      return super.visitPrepare(node, context);
    }

    @Override
    protected Void visitExecute(Execute node, Void context) {
      addNodeSize(node);
      addListSize(node.getParameters());
      return super.visitExecute(node, context);
    }

    @Override
    protected Void visitExecuteImmediate(ExecuteImmediate node, Void context) {
      addNodeSize(node);
      addListSize(node.getParameters());
      return super.visitExecuteImmediate(node, context);
    }

    @Override
    protected Void visitDeallocate(Deallocate node, Void context) {
      addNodeSize(node);
      return super.visitDeallocate(node, context);
    }
  }
}
