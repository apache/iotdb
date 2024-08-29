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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class IrTypeAnalyzer {

  private final PlannerContext plannerContext;

  public IrTypeAnalyzer(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  public Map<NodeRef<Expression>, Type> getTypes(
      SessionInfo session, TypeProvider inputTypes, Iterable<Expression> expressions) {
    Visitor visitor = new Visitor(plannerContext, session, inputTypes);

    for (Expression expression : expressions) {
      visitor.process(expression, new Context(ImmutableMap.of()));
    }

    return visitor.getTypes();
  }

  public Map<NodeRef<Expression>, Type> getTypes(
      SessionInfo session, TypeProvider inputTypes, Expression expression) {
    return getTypes(session, inputTypes, ImmutableList.of(expression));
  }

  public Type getType(SessionInfo session, TypeProvider inputTypes, Expression expression) {
    return getTypes(session, inputTypes, expression).get(NodeRef.of(expression));
  }

  private static class Visitor extends AstVisitor<Type, Context> {
    private static final AccessControl ALLOW_ALL_ACCESS_CONTROL = new AllowAllAccessControl();

    private final PlannerContext plannerContext;
    private final SessionInfo session;
    private final TypeProvider symbolTypes;

    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();

    public Visitor(PlannerContext plannerContext, SessionInfo session, TypeProvider symbolTypes) {
      this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
      this.session = requireNonNull(session, "session is null");
      this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
    }

    public Map<NodeRef<Expression>, Type> getTypes() {
      return expressionTypes;
    }

    private Type setExpressionType(Expression expression, Type type) {
      requireNonNull(expression, "expression cannot be null");
      requireNonNull(type, "type cannot be null");

      expressionTypes.put(NodeRef.of(expression), type);
      return type;
    }

    @Override
    public Type process(Node node, Context context) {
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
    protected Type visitSymbolReference(SymbolReference node, Context context) {
      Symbol symbol = Symbol.from(node);
      Type type = context.getArgumentTypes().get(symbol);
      if (type == null) {
        type = symbolTypes.getTableModelType(symbol);
      }
      checkArgument(type != null, "No type for: %s", node.getName());
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitNotExpression(NotExpression node, Context context) {
      process(node.getValue(), context);
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitLogicalExpression(LogicalExpression node, Context context) {
      node.getTerms().forEach(term -> process(term, context));
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitComparisonExpression(ComparisonExpression node, Context context) {
      process(node.getLeft(), context);
      process(node.getRight(), context);
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitIsNullPredicate(IsNullPredicate node, Context context) {
      process(node.getValue(), context);
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
      process(node.getValue(), context);
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitNullIfExpression(NullIfExpression node, Context context) {
      Type firstType = process(node.getFirst(), context);
      Type ignored = process(node.getSecond(), context);

      // TODO:
      //    NULLIF(v1, v2) = IF(v1 = v2, v1)
      //    In order to compare v1 and v2, they need to have the same (coerced) type, but
      //    the result of NULLIF should be the same as v1. It's currently not possible
      //    to represent this in the IR, so we allow the types to be different for now and
      //    rely on the execution layer to insert the necessary casts.

      return setExpressionType(node, firstType);
    }

    @Override
    protected Type visitIfExpression(IfExpression node, Context context) {
      Type conditionType = process(node.getCondition(), context);
      checkArgument(conditionType.equals(BOOLEAN), "Condition must be boolean: %s", conditionType);

      Type trueType = process(node.getTrueValue(), context);
      if (node.getFalseValue().isPresent()) {
        Type falseType = process(node.getFalseValue().get(), context);
        checkArgument(
            trueType.equals(falseType), "Types must be equal: %s vs %s", trueType, falseType);
      }

      return setExpressionType(node, trueType);
    }

    @Override
    protected Type visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
      Set<Type> resultTypes =
          node.getWhenClauses().stream()
              .map(
                  clause -> {
                    Type operandType = process(clause.getOperand(), context);
                    checkArgument(
                        operandType.equals(BOOLEAN),
                        "When clause operand must be boolean: %s",
                        operandType);
                    return setExpressionType(clause, process(clause.getResult(), context));
                  })
              .collect(Collectors.toSet());

      checkArgument(resultTypes.size() == 1, "All result types must be the same: %s", resultTypes);
      Type resultType = resultTypes.iterator().next();
      node.getDefaultValue()
          .ifPresent(
              defaultValue -> {
                Type defaultType = process(defaultValue, context);
                checkArgument(
                    defaultType.equals(resultType),
                    "Default result type must be the same as WHEN result types: %s vs %s",
                    defaultType,
                    resultType);
              });

      return setExpressionType(node, resultType);
    }

    @Override
    protected Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
      Type operandType = process(node.getOperand(), context);

      Set<Type> resultTypes =
          node.getWhenClauses().stream()
              .map(
                  clause -> {
                    Type clauseOperandType = process(clause.getOperand(), context);
                    checkArgument(
                        clauseOperandType.equals(operandType),
                        "WHEN clause operand type must match CASE operand type: %s vs %s",
                        clauseOperandType,
                        operandType);
                    return setExpressionType(clause, process(clause.getResult(), context));
                  })
              .collect(Collectors.toSet());

      checkArgument(resultTypes.size() == 1, "All result types must be the same: %s", resultTypes);
      Type resultType = resultTypes.iterator().next();
      node.getDefaultValue()
          .ifPresent(
              defaultValue -> {
                Type defaultType = process(defaultValue, context);
                checkArgument(
                    defaultType.equals(resultType),
                    "Default result type must be the same as WHEN result types: %s vs %s",
                    defaultType,
                    resultType);
              });

      return setExpressionType(node, resultType);
    }

    @Override
    protected Type visitCoalesceExpression(CoalesceExpression node, Context context) {
      Set<Type> types =
          node.getOperands().stream()
              .map(operand -> process(operand, context))
              .collect(Collectors.toSet());

      checkArgument(types.size() == 1, "All operands must have the same type: %s", types);
      return setExpressionType(node, types.iterator().next());
    }

    @Override
    protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, Context context) {
      return setExpressionType(node, process(node.getValue(), context));
    }

    @Override
    protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
      argumentTypes.add(process(node.getLeft(), context));
      argumentTypes.add(process(node.getRight(), context));

      try {
        return setExpressionType(
            node,
            plannerContext
                .getMetadata()
                .getOperatorReturnType(
                    OperatorType.valueOf(node.getOperator().name()), argumentTypes.build()));
      } catch (OperatorNotFoundException e) {
        throw new SemanticException(e.getMessage());
      }
    }

    @Override
    protected Type visitStringLiteral(StringLiteral node, Context context) {
      return setExpressionType(node, StringType.STRING);
    }

    @Override
    protected Type visitBinaryLiteral(BinaryLiteral node, Context context) {
      return setExpressionType(node, BlobType.BLOB);
    }

    @Override
    protected Type visitLongLiteral(LongLiteral node, Context context) {
      if (node.getParsedValue() >= Integer.MIN_VALUE
          && node.getParsedValue() <= Integer.MAX_VALUE) {
        return setExpressionType(node, INT32);
      }

      return setExpressionType(node, INT64);
    }

    @Override
    protected Type visitDoubleLiteral(DoubleLiteral node, Context context) {
      return setExpressionType(node, DOUBLE);
    }

    @Override
    protected Type visitBooleanLiteral(BooleanLiteral node, Context context) {
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitGenericLiteral(GenericLiteral node, Context context) {
      Type type;
      if (DateType.DATE.getTypeEnum().name().equals(node.getType())) {
        type = DateType.DATE;
      } else if (TimestampType.TIMESTAMP.getTypeEnum().name().equals(node.getType())) {
        type = TimestampType.TIMESTAMP;
      } else {
        throw new SemanticException("Unsupported type in GenericLiteral: " + node.getType());
      }
      return setExpressionType(node, type);
    }

    @Override
    protected Type visitNullLiteral(NullLiteral node, Context context) {
      return setExpressionType(node, UNKNOWN);
    }

    @Override
    protected Type visitFunctionCall(FunctionCall node, Context context) {
      // Function should already be resolved in IR
      List<Type> argumentTypes = new ArrayList<>(node.getArguments().size());
      for (int i = 0; i < node.getArguments().size(); i++) {
        Expression argument = node.getArguments().get(i);
        argumentTypes.add(process(argument, context));
      }

      return setExpressionType(
          node,
          plannerContext
              .getMetadata()
              .getFunctionReturnType(node.getName().getSuffix(), argumentTypes));
    }

    @Override
    protected Type visitBetweenPredicate(BetweenPredicate node, Context context) {
      process(node.getValue(), context);
      process(node.getMin(), context);
      process(node.getMax(), context);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    public Type visitCast(Cast node, Context context) {
      process(node.getExpression(), context);
      return setExpressionType(
          node, plannerContext.getTypeManager().getType(toTypeSignature(node.getType())));
    }

    @Override
    protected Type visitInPredicate(InPredicate node, Context context) {
      Expression value = node.getValue();
      InListExpression valueList = (InListExpression) node.getValueList();

      Type type = process(value, context);
      for (Expression item : valueList.getValues()) {
        process(item, context);
      }

      setExpressionType(valueList, type);

      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitLikePredicate(LikePredicate node, Context context) {
      process(node.getValue(), context);
      process(node.getPattern(), context);
      node.getEscape().ifPresent(e -> process(e, context));
      return setExpressionType(node, BOOLEAN);
    }

    @Override
    protected Type visitExpression(Expression node, Context context) {
      throw new UnsupportedOperationException(
          "Not a valid IR expression: " + node.getClass().getName());
    }

    @Override
    protected Type visitNode(Node node, Context context) {
      throw new UnsupportedOperationException(
          "Not a valid IR expression: " + node.getClass().getName());
    }
  }

  private static class Context {
    private final Map<Symbol, Type> argumentTypes;

    public Context(Map<Symbol, Type> argumentTypes) {
      this.argumentTypes = argumentTypes;
    }

    public Map<Symbol, Type> getArgumentTypes() {
      return argumentTypes;
    }
  }
}
