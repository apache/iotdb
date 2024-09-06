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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DecimalLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StackableAstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.UnknownType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isTwoTypeComparable;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression.isQualifiedAllFieldsReference;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class ExpressionAnalyzer {

  private final Metadata metadata;
  private final AccessControl accessControl;

  private final BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory;

  private final TypeProvider symbolTypes;

  private final Map<NodeRef<Node>, ResolvedFunction> resolvedFunctions = new LinkedHashMap<>();
  private final Set<NodeRef<SubqueryExpression>> subqueries = new LinkedHashSet<>();
  private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();

  private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();

  private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();
  private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
  private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons =
      new LinkedHashSet<>();

  private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

  // Track referenced fields from source relation node
  private final Multimap<NodeRef<Node>, Field> referencedFields = HashMultimap.create();

  private final MPPQueryContext context;
  private final SessionInfo session;

  private final Map<NodeRef<Parameter>, Expression> parameters;
  private final WarningCollector warningCollector;

  private final Function<Expression, Type> getPreanalyzedType;

  private final List<Field> sourceFields = new ArrayList<>();

  // Record fields prefixed with labels in row pattern recognition context
  private final Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> labelDereferences =
      new LinkedHashMap<>();

  private ExpressionAnalyzer(
      Metadata metadata,
      MPPQueryContext context,
      AccessControl accessControl,
      StatementAnalyzerFactory statementAnalyzerFactory,
      Analysis analysis,
      SessionInfo session,
      TypeProvider types,
      WarningCollector warningCollector) {
    this(
        metadata,
        context,
        accessControl,
        (node, correlationSupport) ->
            statementAnalyzerFactory.createStatementAnalyzer(
                analysis, context, session, warningCollector, correlationSupport),
        session,
        types,
        analysis.getParameters(),
        warningCollector,
        analysis::getType);
  }

  ExpressionAnalyzer(
      Metadata metadata,
      MPPQueryContext context,
      AccessControl accessControl,
      BiFunction<Node, CorrelationSupport, StatementAnalyzer> statementAnalyzerFactory,
      SessionInfo session,
      TypeProvider symbolTypes,
      Map<NodeRef<Parameter>, Expression> parameters,
      WarningCollector warningCollector,
      Function<Expression, Type> getPreanalyzedType) {
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.context = requireNonNull(context, "context is null");
    this.accessControl = requireNonNull(accessControl, "accessControl is null");
    this.statementAnalyzerFactory =
        requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
    this.session = requireNonNull(session, "session is null");
    this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
    this.parameters = requireNonNull(parameters, "parameters is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    this.getPreanalyzedType = requireNonNull(getPreanalyzedType, "getPreanalyzedType is null");
  }

  public Map<NodeRef<Node>, ResolvedFunction> getResolvedFunctions() {
    return unmodifiableMap(resolvedFunctions);
  }

  public Map<NodeRef<Expression>, Type> getExpressionTypes() {
    return unmodifiableMap(expressionTypes);
  }

  public Type setExpressionType(Expression expression, Type type) {
    requireNonNull(expression, "expression cannot be null");
    requireNonNull(type, "type cannot be null");

    expressionTypes.put(NodeRef.of(expression), type);

    return type;
  }

  private Type getExpressionType(Expression expression) {
    requireNonNull(expression, "expression cannot be null");

    Type type = expressionTypes.get(NodeRef.of(expression));
    checkState(type != null, "Expression not yet analyzed: %s", expression);
    return type;
  }

  public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
    return unmodifiableSet(subqueryInPredicates);
  }

  public Map<NodeRef<Expression>, ResolvedField> getColumnReferences() {
    return unmodifiableMap(columnReferences);
  }

  public Type analyze(Expression expression, Scope scope) {
    Visitor visitor = new Visitor(scope, warningCollector);
    return visitor.process(
        expression,
        new StackableAstVisitor.StackableAstVisitorContext<>(
            Context.notInLambda(scope, CorrelationSupport.ALLOWED)));
  }

  public Type analyze(Expression expression, Scope scope, CorrelationSupport correlationSupport) {
    Visitor visitor = new Visitor(scope, warningCollector);
    return visitor.process(
        expression,
        new StackableAstVisitor.StackableAstVisitorContext<>(
            Context.notInLambda(scope, correlationSupport)));
  }

  private Type analyze(Expression expression, Scope scope, Set<String> labels) {
    Visitor visitor = new Visitor(scope, warningCollector);
    return visitor.process(
        expression,
        new StackableAstVisitor.StackableAstVisitorContext<>(
            Context.patternRecognition(scope, labels)));
  }

  private Type analyze(Expression expression, Scope baseScope, Context context) {
    Visitor visitor = new Visitor(baseScope, warningCollector);
    return visitor.process(
        expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
  }

  public Set<NodeRef<SubqueryExpression>> getSubqueries() {
    return unmodifiableSet(subqueries);
  }

  public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
    return unmodifiableSet(existsSubqueries);
  }

  public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
    return unmodifiableSet(quantifiedComparisons);
  }

  public Multimap<QualifiedObjectName, String> getTableColumnReferences() {
    return tableColumnReferences;
  }

  public List<Field> getSourceFields() {
    return sourceFields;
  }

  private class Visitor extends StackableAstVisitor<Type, Context> {
    // Used to resolve FieldReferences (e.g. during local execution planning)
    private final Scope baseScope;
    private final WarningCollector warningCollector;

    public Visitor(Scope baseScope, WarningCollector warningCollector) {
      this.baseScope = requireNonNull(baseScope, "baseScope is null");
      this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    @Override
    public Type process(Node node, @Nullable StackableAstVisitorContext<Context> context) {
      if (node instanceof Expression) {
        // don't double process a node
        Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
        if (type != null) {
          return type;
        }
      }
      return super.process(node, context);
    }

    @Override
    protected Type visitRow(Row node, StackableAstVisitorContext<Context> context) {
      List<Type> types =
          node.getItems().stream().map(child -> process(child, context)).collect(toImmutableList());

      Type type = RowType.anonymous(types);
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitCurrentTime(CurrentTime node, StackableAstVisitorContext<Context> context) {
      if (requireNonNull(node.getFunction()) == CurrentTime.Function.TIMESTAMP) {
        return setExpressionType(node, INT64);
      }
      throw new UnsupportedOperationException(node.toString());
    }

    @Override
    protected Type visitSymbolReference(
        SymbolReference node, StackableAstVisitorContext<Context> context) {
      //      if (context.getContext().isInLambda()) {
      //        Optional<ResolvedField> resolvedField =
      //            context.getContext().getScope().tryResolveField(node,
      // QualifiedName.of(node.getName()));
      //        if (resolvedField.isPresent() &&
      //
      // context.getContext().getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
      //          return setExpressionType(node, resolvedField.get().getType());
      //        }
      //      }
      Type type = symbolTypes.getTableModelType(Symbol.from(node));
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context) {
      ResolvedField resolvedField =
          context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
      return handleResolvedField(node, resolvedField, context);
    }

    private Type handleResolvedField(
        Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context) {
      if (!resolvedField.isLocal()
          && context.getContext().getCorrelationSupport() != CorrelationSupport.ALLOWED) {
        throw new SemanticException(
            String.format(
                "Reference to column '%s' from outer scope not allowed in this context", node));
      }

      FieldId fieldId = FieldId.from(resolvedField);
      Field field = resolvedField.getField();
      //      if (context.getContext().isInLambda()) {
      //        LambdaArgumentDeclaration lambdaArgumentDeclaration =
      //            context.getContext().getFieldToLambdaArgumentDeclaration().get(fieldId);
      //        if (lambdaArgumentDeclaration != null) {
      //          // Lambda argument reference is not a column reference
      //          lambdaArgumentReferences.put(NodeRef.of((Identifier) node),
      // lambdaArgumentDeclaration);
      //          return setExpressionType(node, field.getType());
      //        }
      //      }

      if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
        tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
      }

      sourceFields.add(field);

      fieldId
          .getRelationId()
          .getSourceNode()
          .ifPresent(source -> referencedFields.put(NodeRef.of(source), field));

      ResolvedField previous = columnReferences.put(NodeRef.of(node), resolvedField);
      checkState(previous == null, "%s already known to refer to %s", node, previous);

      return setExpressionType(node, field.getType());
    }

    @Override
    protected Type visitDereferenceExpression(
        DereferenceExpression node, StackableAstVisitorContext<Context> context) {
      if (isQualifiedAllFieldsReference(node)) {
        throw new SemanticException("<identifier>.* not allowed in this context");
      }

      QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

      // If this Dereference looks like column reference, try match it to column first.
      if (qualifiedName != null) {
        Scope scope = context.getContext().getScope();
        Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
        if (resolvedField.isPresent()) {
          return handleResolvedField(node, resolvedField.get(), context);
        }
        if (!scope.isColumnReference(qualifiedName)) {
          throw new SemanticException(
              String.format("Column '%s' cannot be resolved", qualifiedName));
        }
      }

      Type baseType = process(node.getBase(), context);
      if (!(baseType instanceof RowType)) {
        throw new SemanticException(
            String.format("Expression %s is not of type ROW", node.getBase()));
      }

      RowType rowType = (RowType) baseType;

      Identifier field =
          node.getField().orElseThrow(() -> new NoSuchElementException("No value present"));
      String fieldName = field.getValue();

      boolean foundFieldName = false;
      Type rowFieldType = null;
      for (RowType.Field rowField : rowType.getFields()) {
        if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
          if (foundFieldName) {
            throw new SemanticException(
                String.format("Ambiguous row field reference: %s", fieldName));
          }
          foundFieldName = true;
          rowFieldType = rowField.getType();
        }
      }

      if (rowFieldType == null) {
        throw new SemanticException(String.format("Column '%s' cannot be resolved", qualifiedName));
      }

      return setExpressionType(node, rowFieldType);
    }

    @Override
    protected Type visitNotExpression(
        NotExpression node, StackableAstVisitorContext<Context> context) {
      coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitLogicalExpression(
        LogicalExpression node, StackableAstVisitorContext<Context> context) {
      for (Expression term : node.getTerms()) {
        coerceType(context, term, BOOLEAN, "Logical expression term");
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitComparisonExpression(
        ComparisonExpression node, StackableAstVisitorContext<Context> context) {
      OperatorType operatorType = null;
      switch (node.getOperator()) {
        case EQUAL:
        case NOT_EQUAL:
          operatorType = OperatorType.EQUAL;
          break;
        case LESS_THAN:
        case GREATER_THAN:
          operatorType = OperatorType.LESS_THAN;
          break;
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN_OR_EQUAL:
          operatorType = OperatorType.LESS_THAN_OR_EQUAL;
          break;
        case IS_DISTINCT_FROM:
          operatorType = OperatorType.IS_DISTINCT_FROM;
          break;
      }

      return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
    }

    @Override
    protected Type visitIsNullPredicate(
        IsNullPredicate node, StackableAstVisitorContext<Context> context) {
      process(node.getValue(), context);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitIsNotNullPredicate(
        IsNotNullPredicate node, StackableAstVisitorContext<Context> context) {
      process(node.getValue(), context);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitNullIfExpression(
        NullIfExpression node, StackableAstVisitorContext<Context> context) {
      Type firstType = process(node.getFirst(), context);
      Type secondType = process(node.getSecond(), context);

      if (!firstType.equals(secondType)) {
        throw new SemanticException(
            String.format("Types are not comparable with NULLIF: %s vs %s", firstType, secondType));
      }

      return setExpressionType(node, firstType);
    }

    @Override
    protected Type visitIfExpression(
        IfExpression node, StackableAstVisitorContext<Context> context) {
      coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

      Type type;
      if (node.getFalseValue().isPresent()) {
        type =
            coerceToSingleType(
                context,
                node,
                "Result types for IF must be the same",
                node.getTrueValue(),
                node.getFalseValue().get());
      } else {
        type = process(node.getTrueValue(), context);
      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitSearchedCaseExpression(
        SearchedCaseExpression node, StackableAstVisitorContext<Context> context) {
      for (WhenClause whenClause : node.getWhenClauses()) {
        coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
      }

      Type type =
          coerceToSingleType(
              context,
              "All CASE results",
              getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
      setExpressionType(node, type);

      for (WhenClause whenClause : node.getWhenClauses()) {
        Type whenClauseType = process(whenClause.getResult(), context);
        setExpressionType(whenClause, whenClauseType);
      }

      return type;
    }

    @Override
    protected Type visitSimpleCaseExpression(
        SimpleCaseExpression node, StackableAstVisitorContext<Context> context) {
      coerceCaseOperandToToSingleType(node, context);

      Type type =
          coerceToSingleType(
              context,
              "All CASE results",
              getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
      setExpressionType(node, type);

      for (WhenClause whenClause : node.getWhenClauses()) {
        Type whenClauseType = process(whenClause.getResult(), context);
        setExpressionType(whenClause, whenClauseType);
      }

      return type;
    }

    private void coerceCaseOperandToToSingleType(
        SimpleCaseExpression node, StackableAstVisitorContext<Context> context) {
      Type operandType = process(node.getOperand(), context);

      List<WhenClause> whenClauses = node.getWhenClauses();
      List<Type> whenOperandTypes = new ArrayList<>(whenClauses.size());

      for (WhenClause whenClause : whenClauses) {
        Expression whenOperand = whenClause.getOperand();
        Type whenOperandType = process(whenOperand, context);
        whenOperandTypes.add(whenOperandType);

        if (!operandType.equals(whenOperandType)) {
          throw new SemanticException(
              String.format(
                  "CASE operand type does not match WHEN clause operand type: %s vs %s",
                  operandType, whenOperandType));
        }
      }

      for (int i = 0; i < whenOperandTypes.size(); i++) {
        Type whenOperandType = whenOperandTypes.get(i);
        if (!whenOperandType.equals(operandType)) {
          //          Expression whenOperand = whenClauses.get(i).getOperand();
          throw new SemanticException(
              String.format(
                  "CASE operand type does not match WHEN clause operand type: %s vs %s",
                  operandType, whenOperandType));
          //          addOrReplaceExpressionCoercion(whenOperand, whenOperandType, operandType);
        }
      }
    }

    private List<Expression> getCaseResultExpressions(
        List<WhenClause> whenClauses, Optional<Expression> defaultValue) {
      List<Expression> resultExpressions = new ArrayList<>();
      for (WhenClause whenClause : whenClauses) {
        resultExpressions.add(whenClause.getResult());
      }
      defaultValue.ifPresent(resultExpressions::add);
      return resultExpressions;
    }

    @Override
    protected Type visitCoalesceExpression(
        CoalesceExpression node, StackableAstVisitorContext<Context> context) {
      Type type = coerceToSingleType(context, "All COALESCE operands", node.getOperands());

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitArithmeticUnary(
        ArithmeticUnaryExpression node, StackableAstVisitorContext<Context> context) {
      switch (node.getSign()) {
        case PLUS:
          Type type = process(node.getValue(), context);

          if (!isNumericType(type)) {
            // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary
            // operator
            // that types can chose to implement, or piggyback on the existence of the negation
            // operator
            throw new SemanticException(
                String.format("Unary '+' operator cannot by applied to %s type", type));
          }
          return setExpressionType(node, type);
        case MINUS:
          return getOperator(context, node, OperatorType.NEGATION, node.getValue());
        default:
          throw new IllegalArgumentException("Unknown sign: " + node.getSign());
      }
    }

    @Override
    protected Type visitArithmeticBinary(
        ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context) {
      return getOperator(
          context,
          node,
          OperatorType.valueOf(node.getOperator().name()),
          node.getLeft(),
          node.getRight());
    }

    @Override
    protected Type visitLikePredicate(
        LikePredicate node, StackableAstVisitorContext<Context> context) {
      Type valueType = process(node.getValue(), context);
      if (!isCharType(valueType)) {
        throw new SemanticException(
            String.format(
                "Left side of LIKE expression must evaluate to TEXT or STRING Type (actual: %s)",
                valueType));
      }

      Type patternType = process(node.getPattern(), context);
      if (!isCharType(patternType)) {
        throw new SemanticException(
            String.format(
                "Pattern for LIKE expression must evaluate to TEXT or STRING Type (actual: %s)",
                patternType));
      }
      if (node.getEscape().isPresent()) {
        Expression escape = node.getEscape().get();
        Type escapeType = process(escape, context);
        if (!isCharType(escapeType)) {
          throw new SemanticException(
              String.format(
                  "Escape for LIKE expression must evaluate to TEXT or STRING Type (actual: %s)",
                  escapeType));
        }
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitStringLiteral(
        StringLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, STRING);
    }

    @Override
    protected Type visitBinaryLiteral(
        BinaryLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, BLOB);
    }

    @Override
    protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context) {
      if (node.getParsedValue() >= Integer.MIN_VALUE
          && node.getParsedValue() <= Integer.MAX_VALUE) {
        return setExpressionType(node, INT32);
      }

      return setExpressionType(node, INT64);
    }

    @Override
    protected Type visitDoubleLiteral(
        DoubleLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, DOUBLE);
    }

    @Override
    protected Type visitDecimalLiteral(
        DecimalLiteral node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException("DecimalLiteral is not supported yet.");
    }

    @Override
    protected Type visitBooleanLiteral(
        BooleanLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitGenericLiteral(
        GenericLiteral node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException("GenericLiteral is not supported yet.");
    }

    @Override
    protected Type visitNullLiteral(NullLiteral node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, UNKNOWN);
    }

    @Override
    protected Type visitFunctionCall(
        FunctionCall node, StackableAstVisitorContext<Context> context) {
      String functionName = node.getName().getSuffix();
      boolean isAggregation = metadata.isAggregationFunction(session, functionName, accessControl);
      // argument of the form `label.*` is only allowed for row pattern count function
      node.getArguments().stream()
          .filter(DereferenceExpression::isQualifiedAllFieldsReference)
          .findAny()
          .ifPresent(
              allRowsReference -> {
                if (node.getArguments().size() > 1) {
                  throw new SemanticException(
                      "label.* syntax is only supported as the only argument of row pattern count function");
                }
              });

      if (node.isDistinct() && !isAggregation) {
        throw new SemanticException("DISTINCT is not supported for non-aggregation functions");
      }

      List<Type> argumentTypes = getCallArgumentTypes(node.getArguments(), context);

      if (node.getArguments().size() > 127) {
        throw new SemanticException(
            String.format("Too many arguments for function call %s()", functionName));
      }

      for (Type argumentType : argumentTypes) {
        if (node.isDistinct() && !argumentType.isComparable()) {
          throw new SemanticException(
              String.format(
                  "DISTINCT can only be applied to comparable types (actual: %s)", argumentType));
        }
      }

      Type type = metadata.getFunctionReturnType(functionName, argumentTypes);
      // now we only support scalar or agg functions
      ResolvedFunction resolvedFunction =
          new ResolvedFunction(
              new BoundSignature(functionName.toLowerCase(Locale.ENGLISH), type, argumentTypes),
              new FunctionId("noop"),
              isAggregation ? FunctionKind.AGGREGATE : FunctionKind.SCALAR,
              true,
              isAggregation
                  ? FunctionNullability.getAggregationFunctionNullability(argumentTypes.size())
                  : FunctionNullability.getScalarFunctionNullability(argumentTypes.size()));
      resolvedFunctions.put(NodeRef.of(node), resolvedFunction);
      return setExpressionType(node, type);
    }

    public List<Type> getCallArgumentTypes(
        List<Expression> arguments, StackableAstVisitorContext<Context> context) {
      ImmutableList.Builder<Type> argumentTypesBuilder = ImmutableList.builder();
      for (Expression argument : arguments) {
        if (isQualifiedAllFieldsReference(argument)) {
          // to resolve `count(label.*)` correctly, we should skip the argument, like for `count(*)`
          // process the argument but do not include it in the list
          DereferenceExpression allRowsDereference = (DereferenceExpression) argument;
          String label = label((Identifier) allRowsDereference.getBase());
          if (!context.getContext().getLabels().contains(label)) {
            throw new SemanticException(
                String.format("%s is not a primary pattern variable or subset name", label));
          }
          labelDereferences.put(NodeRef.of(allRowsDereference), new LabelPrefixedReference(label));
        } else {
          argumentTypesBuilder.add(process(argument, context));
        }
      }

      return argumentTypesBuilder.build();
    }

    private String label(Identifier identifier) {
      return identifier.getCanonicalValue();
    }

    @Override
    protected Type visitCurrentDatabase(
        CurrentDatabase node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, STRING);
    }

    @Override
    protected Type visitCurrentUser(CurrentUser node, StackableAstVisitorContext<Context> context) {
      return setExpressionType(node, STRING);
    }

    @Override
    protected Type visitTrim(Trim node, StackableAstVisitorContext<Context> context) {
      ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();

      argumentTypes.add(process(node.getTrimSource(), context));
      node.getTrimCharacter().ifPresent(trimChar -> argumentTypes.add(process(trimChar, context)));
      List<Type> actualTypes = argumentTypes.build();

      String functionName = node.getSpecification().getFunctionName();

      Type returnType = metadata.getFunctionReturnType(functionName, actualTypes);

      return setExpressionType(node, returnType);
    }

    @Override
    protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context) {

      if (parameters.isEmpty()) {
        throw new SemanticException("Query takes no parameters");
      }
      if (node.getId() >= parameters.size()) {
        throw new SemanticException(
            String.format(
                "Invalid parameter index %s, max value is %s",
                node.getId(), parameters.size() - 1));
      }

      Expression providedValue = parameters.get(NodeRef.of(node));
      if (providedValue == null) {
        throw new SemanticException("No value provided for parameter");
      }
      Type resultType = process(providedValue, context);
      return setExpressionType(node, resultType);
    }

    @Override
    protected Type visitBetweenPredicate(
        BetweenPredicate node, StackableAstVisitorContext<Context> context) {
      Type valueType = process(node.getValue(), context);
      Type minType = process(node.getMin(), context);
      Type maxType = process(node.getMax(), context);

      if (!isTwoTypeComparable(Arrays.asList(valueType, minType))
          || !isTwoTypeComparable(Arrays.asList(valueType, maxType))) {
        throw new SemanticException(
            String.format("Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType));
      }

      if (!valueType.isOrderable()) {
        throw new SemanticException(
            String.format("Cannot check if %s is BETWEEN %s and %s", valueType, minType, maxType));
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    public Type visitCast(Cast node, StackableAstVisitorContext<Context> context) {

      Type type;
      try {
        type = metadata.getType(toTypeSignature(node.getType()));
      } catch (TypeNotFoundException e) {
        throw new SemanticException(String.format("Unknown type: %s", node.getType()));
      }

      if (type.equals(UnknownType.UNKNOWN)) {
        throw new SemanticException("UNKNOWN is not a valid type");
      }

      Type value = process(node.getExpression(), context);
      if (!value.equals(UnknownType.UNKNOWN)
          && !node.isTypeOnly()
          && (!metadata.canCoerce(value, type))) {
        throw new SemanticException(String.format("Cannot cast %s to %s", value, type));
      }

      return setExpressionType(node, type);
    }

    @Override
    protected Type visitInPredicate(InPredicate node, StackableAstVisitorContext<Context> context) {
      Expression value = node.getValue();
      Expression valueList = node.getValueList();

      // When an IN-predicate containing a subquery: `x IN (SELECT ...)` is planned, both `value`
      // and `valueList` are pre-planned.
      // In the row pattern matching context, expressions can contain labeled column references,
      // navigations, CALSSIFIER(), and MATCH_NUMBER() calls.
      // None of these can be pre-planned. Instead, the query fails:
      // - if such elements are in the `value list` (subquery), the analysis of the subquery fails
      // as it is done in a non-pattern-matching context.
      // - if such elements are in `value`, they are captured by the below check.
      //
      // NOTE: Theoretically, if the IN-predicate does not contain CLASSIFIER() or MATCH_NUMBER()
      // calls, it could be pre-planned
      // on the condition that all column references of the `value` are consistently navigated
      // (i.e., the expression is effectively evaluated within a single row),
      // and that the same navigation should be applied to the resulting symbol.
      // Currently, we only support the case when there are no explicit labels or navigations. This
      // is a special case of such
      // consistent navigating, as the column reference `x` defaults to `RUNNING
      // LAST(universal_pattern_variable.x)`.

      if (valueList instanceof InListExpression) {
        InListExpression inListExpression = (InListExpression) valueList;
        Type type =
            coerceToSingleType(
                context,
                "IN value and list items",
                ImmutableList.<Expression>builder()
                    .add(value)
                    .addAll(inListExpression.getValues())
                    .build());
        setExpressionType(inListExpression, type);
      } else if (valueList instanceof SubqueryExpression) {
        subqueryInPredicates.add(NodeRef.of(node));
        analyzePredicateWithSubquery(
            node, process(value, context), (SubqueryExpression) valueList, context);
      } else {
        throw new IllegalArgumentException(
            "Unexpected value list type for InPredicate: "
                + node.getValueList().getClass().getName());
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitSubqueryExpression(
        SubqueryExpression node, StackableAstVisitorContext<Context> context) {
      Type type = analyzeSubquery(node, context);

      // the implied type of a scalar subquery is that of the unique field in the single-column row
      if (type instanceof RowType && ((RowType) type).getFields().size() == 1) {
        type = type.getTypeParameters().get(0);
      }

      setExpressionType(node, type);
      subqueries.add(NodeRef.of(node));
      return type;
    }

    /**
     * @return the common supertype between the value type and subquery type
     */
    private Type analyzePredicateWithSubquery(
        Expression node,
        Type declaredValueType,
        SubqueryExpression subquery,
        StackableAstVisitorContext<Context> context) {
      Type valueRowType = declaredValueType;
      if (!(declaredValueType instanceof RowType) && !(declaredValueType instanceof UnknownType)) {
        valueRowType = RowType.anonymous(ImmutableList.of(declaredValueType));
      }

      Type subqueryType = analyzeSubquery(subquery, context);
      setExpressionType(subquery, subqueryType);

      if (subqueryType.equals(valueRowType)) {
        throw new SemanticException(
            String.format(
                "Value expression and result of subquery must be of the same type: %s vs %s",
                valueRowType, subqueryType));
      }

      return subqueryType;
    }

    private Type analyzeSubquery(
        SubqueryExpression node, StackableAstVisitorContext<Context> context) {
      StatementAnalyzer analyzer =
          statementAnalyzerFactory.apply(node, context.getContext().getCorrelationSupport());
      Scope subqueryScope = Scope.builder().withParent(context.getContext().getScope()).build();
      Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

      ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
      for (int i = 0; i < queryScope.getRelationType().getAllFieldCount(); i++) {
        Field field = queryScope.getRelationType().getFieldByIndex(i);
        if (!field.isHidden()) {
          if (field.getName().isPresent()) {
            fields.add(RowType.field(field.getName().get(), field.getType()));
          } else {
            fields.add(RowType.field(field.getType()));
          }
        }
      }

      sourceFields.addAll(queryScope.getRelationType().getVisibleFields());
      return RowType.from(fields.build());
    }

    @Override
    protected Type visitExists(ExistsPredicate node, StackableAstVisitorContext<Context> context) {
      StatementAnalyzer analyzer =
          statementAnalyzerFactory.apply(node, context.getContext().getCorrelationSupport());
      Scope subqueryScope = Scope.builder().withParent(context.getContext().getScope()).build();

      List<RowType.Field> fields =
          analyzer
              .analyze(node.getSubquery(), subqueryScope)
              .getRelationType()
              .getAllFields()
              .stream()
              .map(
                  field -> {
                    if (field.getName().isPresent()) {
                      return RowType.field(field.getName().get(), field.getType());
                    }

                    return RowType.field(field.getType());
                  })
              .collect(toImmutableList());

      // TODO: this should be multiset(row(...))
      setExpressionType(node.getSubquery(), RowType.from(fields));

      existsSubqueries.add(NodeRef.of(node));

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitQuantifiedComparisonExpression(
        QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context) {
      quantifiedComparisons.add(NodeRef.of(node));

      Type declaredValueType = process(node.getValue(), context);
      Type comparisonType =
          analyzePredicateWithSubquery(
              node, declaredValueType, (SubqueryExpression) node.getSubquery(), context);

      switch (node.getOperator()) {
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          if (!comparisonType.isOrderable()) {
            throw new SemanticException(
                String.format(
                    "Type [%s] must be orderable in order to be used in quantified comparison",
                    comparisonType));
          }
          break;
        case EQUAL:
        case NOT_EQUAL:
          if (!comparisonType.isComparable()) {
            throw new SemanticException(
                String.format(
                    "Type [%s] must be comparable in order to be used in quantified comparison",
                    comparisonType));
          }
          break;
        default:
          throw new IllegalStateException(
              format("Unexpected comparison type: %s", node.getOperator()));
      }

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    public Type visitFieldReference(
        FieldReference node, StackableAstVisitorContext<Context> context) {
      ResolvedField field = baseScope.getField(node.getFieldIndex());
      return handleResolvedField(node, field, context);
    }

    @Override
    protected Type visitExpression(Expression node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException(
          String.format("not yet implemented: %s", node.getClass().getName()));
    }

    @Override
    protected Type visitNode(Node node, StackableAstVisitorContext<Context> context) {
      throw new SemanticException(
          String.format("not yet implemented: %s", node.getClass().getName()));
    }

    private Type getOperator(
        StackableAstVisitorContext<Context> context,
        Expression node,
        OperatorType operatorType,
        Expression... arguments) {
      ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
      for (Expression expression : arguments) {
        argumentTypes.add(process(expression, context));
      }

      Type type;
      try {
        type = metadata.getOperatorReturnType(operatorType, argumentTypes.build());
      } catch (OperatorNotFoundException e) {
        throw new SemanticException(e.getMessage());
      }

      return setExpressionType(node, type);
    }

    private void coerceType(
        Expression expression, Type actualType, Type expectedType, String message) {
      if (!actualType.equals(expectedType)) {
        //        if (!typeCoercion.canCoerce(actualType, expectedType)) {
        throw new SemanticException(
            String.format(
                "%s must evaluate to a %s (actual: %s)", message, expectedType, actualType));
        //        }
        //        addOrReplaceExpressionCoercion(expression, actualType, expectedType);
      }
    }

    private void coerceType(
        StackableAstVisitorContext<Context> context,
        Expression expression,
        Type expectedType,
        String message) {
      Type actualType = process(expression, context);
      coerceType(expression, actualType, expectedType, message);
    }

    private Type coerceToSingleType(
        StackableAstVisitorContext<Context> context,
        Node node,
        String message,
        Expression first,
        Expression second) {
      Type firstType = UNKNOWN;
      if (first != null) {
        firstType = process(first, context);
      }
      Type secondType = UNKNOWN;
      if (second != null) {
        secondType = process(second, context);
      }

      if (!firstType.equals(secondType)) {
        throw new SemanticException(String.format("%s: %s vs %s", message, firstType, secondType));
      }

      return firstType;
    }

    private Type coerceToSingleType(
        StackableAstVisitorContext<Context> context,
        String description,
        List<Expression> expressions) {
      // determine super type
      Type superType = UNKNOWN;

      // Use LinkedHashMultimap to preserve order in which expressions are analyzed within IN list
      Multimap<Type, NodeRef<Expression>> typeExpressions = LinkedHashMultimap.create();
      for (Expression expression : expressions) {
        // We need to wrap as NodeRef since LinkedHashMultimap does not allow duplicated values
        Type type = process(expression, context);
        typeExpressions.put(type, NodeRef.of(expression));
      }

      Set<Type> types = typeExpressions.keySet();

      for (Type type : types) {
        if (superType == UNKNOWN) {
          superType = type;
        } else {
          if (!isTwoTypeComparable(Arrays.asList(superType, type))) {
            throw new SemanticException(
                String.format(
                    "%s must be the same type or coercible to a common type. Cannot find common type between %s and %s, all types (without duplicates): %s",
                    description, superType, type, typeExpressions.keySet()));
          }
        }
        //        Optional<Type> newSuperType = typeCoercion.getCommonSuperType(superType, type);
        //        if (newSuperType.isEmpty()) {
        //          throw semanticException(TYPE_MISMATCH, Iterables.get(typeExpressions.get(type),
        // 0).getNode(),
        //              "%s must be the same type or coercible to a common type. Cannot find common
        // type between %s and %s, all types (without duplicates): %s",
        //              description,
        //              superType,
        //              type,
        //              typeExpressions.keySet());
        //        }
        //        superType = newSuperType.get();
      }

      // verify all expressions can be coerced to the superType
      //      for (Type type : types) {
      //        Collection<NodeRef<Expression>> coercionCandidates = typeExpressions.get(type);

      //        if (!type.equals(superType)) {
      //          if (!typeCoercion.canCoerce(type, superType)) {

      //          }
      //          addOrReplaceExpressionsCoercion(coercionCandidates, type, superType);
      //        }
      //      }

      return superType;
    }

    //    private void addOrReplaceExpressionCoercion(Expression expression, Type type, Type
    // superType) {
    //      addOrReplaceExpressionsCoercion(ImmutableList.of(NodeRef.of(expression)), type,
    // superType);
    //    }
    //
    //    private void addOrReplaceExpressionsCoercion(Collection<NodeRef<Expression>> expressions,
    // Type type,
    //                                                 Type superType) {
    //      expressions.forEach(expression -> expressionCoercions.put(expression, superType));
    //      if (typeCoercion.isTypeOnlyCoercion(type, superType)) {
    //        typeOnlyCoercions.addAll(expressions);
    //      } else {
    //        typeOnlyCoercions.removeAll(expressions);
    //      }
    //    }
  }

  private static class Context {
    private final Scope scope;

    // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All
    // 4 combinations are possible.

    // The list of types when expecting a lambda (i.e. processing lambda parameters of a function);
    // null otherwise.
    // Empty list represents expecting a lambda with no arguments.
    private final List<Type> functionInputTypes;
    // The mapping from names to corresponding lambda argument declarations when inside a lambda;
    // null otherwise.
    // Empty map means that the all lambda expressions surrounding the current node has no
    // arguments.
    //    private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

    // Primary row pattern variables and named unions (subsets) of variables
    // necessary for the analysis of expressions in the context of row pattern recognition
    private final Set<String> labels;

    private final CorrelationSupport correlationSupport;

    private Context(
        Scope scope,
        List<Type> functionInputTypes,
        Set<String> labels,
        CorrelationSupport correlationSupport) {
      this.scope = requireNonNull(scope, "scope is null");
      this.functionInputTypes = functionInputTypes;
      //      this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
      this.labels = labels;
      this.correlationSupport = requireNonNull(correlationSupport, "correlationSupport is null");
    }

    public static Context notInLambda(Scope scope, CorrelationSupport correlationSupport) {
      return new Context(scope, null, null, correlationSupport);
    }

    public Context expectingLambda(List<Type> functionInputTypes) {
      return new Context(
          scope,
          requireNonNull(functionInputTypes, "functionInputTypes is null"),
          labels,
          correlationSupport);
    }

    public Context notExpectingLambda() {
      return new Context(scope, null, labels, correlationSupport);
    }

    public static Context patternRecognition(Scope scope, Set<String> labels) {
      return new Context(
          scope, null, requireNonNull(labels, "labels is null"), CorrelationSupport.DISALLOWED);
    }

    public Context patternRecognition(Set<String> labels) {
      return new Context(
          scope,
          functionInputTypes,
          requireNonNull(labels, "labels is null"),
          CorrelationSupport.DISALLOWED);
    }

    public Context notExpectingLabels() {
      return new Context(scope, functionInputTypes, null, correlationSupport);
    }

    Scope getScope() {
      return scope;
    }

    //    public boolean isInLambda() {
    //      return fieldToLambdaArgumentDeclaration != null;
    //    }

    public boolean isExpectingLambda() {
      return functionInputTypes != null;
    }

    public List<Type> getFunctionInputTypes() {
      checkState(isExpectingLambda());
      return functionInputTypes;
    }

    public Set<String> getLabels() {
      return labels;
    }

    public CorrelationSupport getCorrelationSupport() {
      return correlationSupport;
    }
  }

  public static ExpressionAnalysis analyzeExpressions(
      Metadata metadata,
      MPPQueryContext context,
      SessionInfo session,
      StatementAnalyzerFactory statementAnalyzerFactory,
      AccessControl accessControl,
      TypeProvider types,
      Iterable<Expression> expressions,
      Map<NodeRef<Parameter>, Expression> parameters,
      WarningCollector warningCollector) {
    Analysis analysis = new Analysis(null, parameters);
    analysis.setDatabaseName(session.getDatabaseName().get());
    ExpressionAnalyzer analyzer =
        new ExpressionAnalyzer(
            metadata,
            context,
            accessControl,
            statementAnalyzerFactory,
            analysis,
            session,
            types,
            warningCollector);
    for (Expression expression : expressions) {
      analyzer.analyze(
          expression,
          Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build());
    }

    return new ExpressionAnalysis(
        analyzer.getExpressionTypes(),
        analyzer.getSubqueryInPredicates(),
        analyzer.getSubqueries(),
        analyzer.getExistsSubqueries(),
        analyzer.getColumnReferences(),
        analyzer.getQuantifiedComparisons());
  }

  public static ExpressionAnalysis analyzeExpression(
      Metadata metadata,
      MPPQueryContext context,
      SessionInfo session,
      StatementAnalyzerFactory statementAnalyzerFactory,
      AccessControl accessControl,
      Scope scope,
      Analysis analysis,
      Expression expression,
      WarningCollector warningCollector,
      CorrelationSupport correlationSupport) {
    ExpressionAnalyzer analyzer =
        new ExpressionAnalyzer(
            metadata,
            context,
            accessControl,
            statementAnalyzerFactory,
            analysis,
            session,
            TypeProvider.empty(),
            warningCollector);
    analyzer.analyze(expression, scope, correlationSupport);

    updateAnalysis(analysis, analyzer, session, accessControl);
    analysis.addExpressionFields(expression, analyzer.getSourceFields());

    return new ExpressionAnalysis(
        analyzer.getExpressionTypes(),
        analyzer.getSubqueryInPredicates(),
        analyzer.getSubqueries(),
        analyzer.getExistsSubqueries(),
        analyzer.getColumnReferences(),
        analyzer.getQuantifiedComparisons());
  }

  public static void analyzeExpressionWithoutSubqueries(
      Metadata metadata,
      MPPQueryContext context,
      SessionInfo session,
      AccessControl accessControl,
      Scope scope,
      Analysis analysis,
      Expression expression,
      String message,
      WarningCollector warningCollector,
      CorrelationSupport correlationSupport) {
    ExpressionAnalyzer analyzer =
        new ExpressionAnalyzer(
            metadata,
            context,
            accessControl,
            (node, ignored) -> {
              throw new SemanticException(message);
            },
            session,
            TypeProvider.empty(),
            analysis.getParameters(),
            warningCollector,
            analysis::getType);
    analyzer.analyze(expression, scope, correlationSupport);

    updateAnalysis(analysis, analyzer, session, accessControl);
    analysis.addExpressionFields(expression, analyzer.getSourceFields());
  }

  private static void updateAnalysis(
      Analysis analysis,
      ExpressionAnalyzer analyzer,
      SessionInfo session,
      AccessControl accessControl) {
    analysis.addTypes(analyzer.getExpressionTypes());
    analyzer
        .getResolvedFunctions()
        .forEach(
            (key, value) ->
                analysis.addResolvedFunction(key.getNode(), value, session.getUserName()));
    analysis.addColumnReferences(analyzer.getColumnReferences());
    analysis.addTableColumnReferences(
        accessControl, session.getIdentity(), analyzer.getTableColumnReferences());
  }

  public static ExpressionAnalyzer createConstantAnalyzer(
      Metadata metadata,
      MPPQueryContext context,
      AccessControl accessControl,
      SessionInfo session,
      Map<NodeRef<Parameter>, Expression> parameters,
      WarningCollector warningCollector) {
    return createWithoutSubqueries(
        metadata,
        context,
        accessControl,
        session,
        parameters,
        "Constant expression cannot contain a subquery",
        warningCollector);
  }

  public static ExpressionAnalyzer createWithoutSubqueries(
      Metadata metadata,
      MPPQueryContext context,
      AccessControl accessControl,
      SessionInfo session,
      Map<NodeRef<Parameter>, Expression> parameters,
      String message,
      WarningCollector warningCollector) {
    return createWithoutSubqueries(
        metadata,
        context,
        accessControl,
        session,
        TypeProvider.empty(),
        parameters,
        node -> new SemanticException(message),
        warningCollector);
  }

  public static ExpressionAnalyzer createWithoutSubqueries(
      Metadata metadata,
      MPPQueryContext context,
      AccessControl accessControl,
      SessionInfo session,
      TypeProvider symbolTypes,
      Map<NodeRef<Parameter>, Expression> parameters,
      Function<Node, ? extends RuntimeException> statementAnalyzerRejection,
      WarningCollector warningCollector) {
    return new ExpressionAnalyzer(
        metadata,
        context,
        accessControl,
        (node, correlationSupport) -> {
          throw statementAnalyzerRejection.apply(node);
        },
        session,
        symbolTypes,
        parameters,
        warningCollector,
        expression -> {
          throw new IllegalStateException("Cannot access preanalyzed types");
        });
  }

  private static boolean isExactNumericWithScaleZero(Type type) {
    return type.equals(INT32) || type.equals(INT64);
  }

  public static class LabelPrefixedReference {
    private final String label;
    private final Optional<Identifier> column;

    public LabelPrefixedReference(String label, Identifier column) {
      this(label, Optional.of(requireNonNull(column, "column is null")));
    }

    public LabelPrefixedReference(String label) {
      this(label, Optional.empty());
    }

    private LabelPrefixedReference(String label, Optional<Identifier> column) {
      this.label = requireNonNull(label, "label is null");
      this.column = requireNonNull(column, "column is null");
    }

    public String getLabel() {
      return label;
    }

    public Optional<Identifier> getColumn() {
      return column;
    }
  }
}
