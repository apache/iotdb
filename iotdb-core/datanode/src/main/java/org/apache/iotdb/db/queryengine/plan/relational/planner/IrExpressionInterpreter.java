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
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.helper.CastFunctionHelper;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.function.InterpretedFunctionInvoker;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeCoercion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.isEffectivelyLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression.Sign.MINUS;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression.Sign.PLUS;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;

public class IrExpressionInterpreter {

  private final Expression expression;
  private final PlannerContext plannerContext;
  private final Metadata metadata;
  private final LiteralInterpreter literalInterpreter;
  private final LiteralEncoder literalEncoder;
  private final SessionInfo session;
  private final Map<NodeRef<Expression>, Type> expressionTypes;
  private final InterpretedFunctionInvoker functionInvoker;
  private final TypeCoercion typeCoercion;

  private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();

  public IrExpressionInterpreter(
      Expression expression,
      PlannerContext plannerContext,
      SessionInfo session,
      Map<NodeRef<Expression>, Type> expressionTypes) {
    this.expression = requireNonNull(expression, "expression is null");
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.metadata = plannerContext.getMetadata();
    this.literalInterpreter = new LiteralInterpreter(plannerContext, session);
    this.literalEncoder = new LiteralEncoder(plannerContext);
    this.session = requireNonNull(session, "session is null");
    this.expressionTypes =
        ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
    verify(expressionTypes.containsKey(NodeRef.of(expression)));
    this.functionInvoker = new InterpretedFunctionInvoker();
    this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
  }

  public static Object evaluateConstantExpression(
      Expression expression, PlannerContext plannerContext, SessionInfo session) {
    Map<NodeRef<Expression>, Type> types =
        new IrTypeAnalyzer(plannerContext).getTypes(session, TypeProvider.empty(), expression);
    return new IrExpressionInterpreter(expression, plannerContext, session, types).evaluate();
  }

  public Object evaluate() {
    Object result = new Visitor(false).processWithExceptionHandling(expression, null);
    verify(
        !(result instanceof Expression),
        "Expression interpreter returned an unresolved expression");
    return result;
  }

  public Object evaluate(SymbolResolver inputs) {
    Object result = new Visitor(false).processWithExceptionHandling(expression, inputs);
    verify(
        !(result instanceof Expression),
        "Expression interpreter returned an unresolved expression");
    return result;
  }

  public Object optimize(SymbolResolver inputs) {
    return new Visitor(true).processWithExceptionHandling(expression, inputs);
  }

  private class Visitor extends AstVisitor<Object, Object> {
    private final boolean optimize;

    private Visitor(boolean optimize) {
      this.optimize = optimize;
    }

    private Object processWithExceptionHandling(Expression expression, Object context) {
      if (expression == null) {
        return null;
      }

      try {
        return process(expression, context);
      } catch (SemanticException e) {
        if (optimize) {
          // Certain operations like 0 / 0 or likeExpression may throw exceptions.
          // When optimizing, do not throw the exception, but delay it until the expression is
          // actually executed.
          // This is to take advantage of the possibility that some other optimization removes the
          // erroneous
          // expression from the plan.
          return expression;
        }
        // Do not suppress exceptions during expression execution.
        throw e;
      }
    }

    @Override
    protected Object visitSymbolReference(SymbolReference node, Object context) {
      return ((SymbolResolver) context).getValue(Symbol.from(node));
    }

    @Override
    protected Object visitLiteral(Literal node, Object context) {
      return literalInterpreter.evaluate(node, type(node));
    }

    @Override
    protected Object visitIsNullPredicate(IsNullPredicate node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);

      if (value instanceof Expression) {
        return new IsNullPredicate(toExpression(value, type(node.getValue())));
      }

      return value == null;
    }

    @Override
    protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);

      if (value instanceof Expression) {
        return new IsNotNullPredicate(toExpression(value, type(node.getValue())));
      }

      return value != null;
    }

    @Override
    protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context) {
      Object newDefault = null;
      boolean foundNewDefault = false;

      List<WhenClause> whenClauses = new ArrayList<>();
      for (WhenClause whenClause : node.getWhenClauses()) {
        Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

        if (whenOperand instanceof Expression) {
          // cannot fully evaluate, add updated whenClause
          whenClauses.add(
              new WhenClause(
                  toExpression(whenOperand, type(whenClause.getOperand())),
                  toExpression(
                      processWithExceptionHandling(whenClause.getResult(), context),
                      type(whenClause.getResult()))));
        } else if (Boolean.TRUE.equals(whenOperand)) {
          // condition is true, use this as default
          foundNewDefault = true;
          newDefault = processWithExceptionHandling(whenClause.getResult(), context);
          break;
        }
      }

      Object defaultResult;
      if (foundNewDefault) {
        defaultResult = newDefault;
      } else {
        defaultResult = processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
      }

      if (whenClauses.isEmpty()) {
        return defaultResult;
      }

      Expression defaultExpression =
          (defaultResult == null) ? null : toExpression(defaultResult, type(node));
      return defaultExpression == null
          ? new SearchedCaseExpression(whenClauses)
          : new SearchedCaseExpression(whenClauses, defaultExpression);
    }

    @Override
    protected Object visitIfExpression(IfExpression node, Object context) {
      Object condition = processWithExceptionHandling(node.getCondition(), context);

      if (condition instanceof Expression) {
        Object trueValue = processWithExceptionHandling(node.getTrueValue(), context);
        Object falseValue =
            processWithExceptionHandling(node.getFalseValue().orElse(null), context);
        return new IfExpression(
            toExpression(condition, type(node.getCondition())),
            toExpression(trueValue, type(node.getTrueValue())),
            (falseValue == null)
                ? null
                : toExpression(falseValue, type(node.getFalseValue().get())));
      }
      if (Boolean.TRUE.equals(condition)) {
        return processWithExceptionHandling(node.getTrueValue(), context);
      }
      return processWithExceptionHandling(node.getFalseValue().orElse(null), context);
    }

    @Override
    protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Object context) {
      Object operand = processWithExceptionHandling(node.getOperand(), context);
      Type operandType = type(node.getOperand());

      // if operand is null, return defaultValue
      if (operand == null) {
        return processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
      }

      Object newDefault = null;
      boolean foundNewDefault = false;

      List<WhenClause> whenClauses = new ArrayList<>();
      for (WhenClause whenClause : node.getWhenClauses()) {
        Object whenOperand = processWithExceptionHandling(whenClause.getOperand(), context);

        if (whenOperand instanceof Expression || operand instanceof Expression) {
          // cannot fully evaluate, add updated whenClause
          whenClauses.add(
              new WhenClause(
                  toExpression(whenOperand, type(whenClause.getOperand())),
                  toExpression(
                      processWithExceptionHandling(whenClause.getResult(), context),
                      type(whenClause.getResult()))));
        } else if (whenOperand != null
            && isEqual(operand, operandType, whenOperand, type(whenClause.getOperand()))) {
          // condition is true, use this as default
          foundNewDefault = true;
          newDefault = processWithExceptionHandling(whenClause.getResult(), context);
          break;
        }
      }

      Object defaultResult;
      if (foundNewDefault) {
        defaultResult = newDefault;
      } else {
        defaultResult = processWithExceptionHandling(node.getDefaultValue().orElse(null), context);
      }

      if (whenClauses.isEmpty()) {
        return defaultResult;
      }

      Expression defaultExpression =
          (defaultResult == null) ? null : toExpression(defaultResult, type(node));
      return defaultExpression == null
          ? new SimpleCaseExpression(toExpression(operand, type(node.getOperand())), whenClauses)
          : new SimpleCaseExpression(
              toExpression(operand, type(node.getOperand())), whenClauses, defaultExpression);
    }

    private boolean isEqual(Object operand1, Type type1, Object operand2, Type type2) {
      return Boolean.TRUE.equals(
          invokeOperator(
              OperatorType.EQUAL,
              ImmutableList.of(type1, type2),
              ImmutableList.of(operand1, operand2)));
    }

    private Type type(Expression expression) {
      Type type = expressionTypes.get(NodeRef.of(expression));
      checkState(type != null, "Type not found for expression: %s", expression);
      return type;
    }

    @Override
    protected Object visitCoalesceExpression(CoalesceExpression node, Object context) {
      List<Object> newOperands = processOperands(node, context);
      if (newOperands.isEmpty()) {
        return null;
      }
      if (newOperands.size() == 1) {
        return getOnlyElement(newOperands);
      }
      return new CoalesceExpression(
          newOperands.stream()
              .map(value -> toExpression(value, type(node)))
              .collect(toImmutableList()));
    }

    private List<Object> processOperands(CoalesceExpression node, Object context) {
      List<Object> newOperands = new ArrayList<>();
      Set<Expression> uniqueNewOperands = new HashSet<>();
      for (Expression operand : node.getOperands()) {
        Object value = processWithExceptionHandling(operand, context);
        if (value instanceof CoalesceExpression) {
          // The nested CoalesceExpression was recursively processed. It does not contain null.
          for (Expression nestedOperand : ((CoalesceExpression) value).getOperands()) {
            // Skip duplicates unless they are non-deterministic.
            if (!isDeterministic(nestedOperand) || uniqueNewOperands.add(nestedOperand)) {
              newOperands.add(nestedOperand);
            }
            // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
            if (isEffectivelyLiteral(nestedOperand, plannerContext, session)) {
              verify(
                  !(nestedOperand instanceof NullLiteral)
                      && !(nestedOperand instanceof Cast
                          && ((Cast) nestedOperand).getExpression() instanceof NullLiteral),
                  "Null operand should have been removed by recursive coalesce processing");
              return newOperands;
            }
          }
        } else if (value instanceof Expression) {
          Expression expr = (Expression) value;
          verify(
              !(value instanceof NullLiteral),
              "Null value is expected to be represented as null, not NullLiteral");
          // Skip duplicates unless they are non-deterministic.
          if (!isDeterministic(expr) || uniqueNewOperands.add(expr)) {
            newOperands.add(expr);
          }
        } else if (value != null) {
          // This operand can be evaluated to a non-null value. Remaining operands can be skipped.
          newOperands.add(value);
          return newOperands;
        }
      }
      return newOperands;
    }

    @Override
    protected Object visitInPredicate(InPredicate node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);

      InListExpression valueList = (InListExpression) node.getValueList();
      // `NULL IN ()` would be false, but InListExpression cannot be empty by construction
      if (value == null) {
        return null;
      }

      if (!(value instanceof Expression)) {
        Set<?> set = inListCache.get(valueList);

        // We use the presence of the node in the map to indicate that we've already done
        // the analysis below. If the value is null, it means that we can't apply the HashSet
        // optimization
        if (!inListCache.containsKey(valueList)) {
          if (valueList.getValues().stream().allMatch(Literal.class::isInstance)
              && valueList.getValues().stream().noneMatch(NullLiteral.class::isInstance)) {
            set =
                valueList.getValues().stream()
                    .map(expr -> processWithExceptionHandling(expr, context))
                    .collect(Collectors.toSet());
          }
          inListCache.put(valueList, set);
        }

        if (set != null) {
          return set.contains(value);
        }
      }

      boolean hasUnresolvedValue = value instanceof Expression;
      boolean hasNullValue = false;
      boolean found = false;
      List<Object> values = new ArrayList<>(valueList.getValues().size());
      List<Type> types = new ArrayList<>(valueList.getValues().size());

      for (Expression expr : valueList.getValues()) {
        if (value instanceof Expression && expr instanceof Literal) {
          // skip interpreting of literal IN term since it cannot be compared
          // with unresolved "value" and it cannot be simplified further
          values.add(expr);
          types.add(type(expr));
          continue;
        }

        // Use process() instead of processWithExceptionHandling() for processing in-list items.
        // Do not handle exceptions thrown while processing a single in-list expression,
        // but fail the whole in-predicate evaluation.
        // According to in-predicate semantics, all in-list items must be successfully evaluated
        // before a check for the match is performed.
        Object inValue = process(expr, context);
        if (value instanceof Expression || inValue instanceof Expression) {
          hasUnresolvedValue = true;
          values.add(inValue);
          types.add(type(expr));
          continue;
        }

        if (inValue == null) {
          hasNullValue = true;
        } else {
          Boolean result = value.equals(inValue);
          if (result == null) {
            hasNullValue = true;
          } else if (!found && result) {
            // in does not short-circuit so we must evaluate all value in the list
            found = true;
          }
        }
      }
      if (found) {
        return true;
      }

      if (hasUnresolvedValue) {
        Type type = type(node.getValue());
        List<Expression> expressionValues = toExpressions(values, types);
        List<Expression> simplifiedExpressionValues =
            Stream.concat(
                    expressionValues.stream()
                        .filter(DeterminismEvaluator::isDeterministic)
                        .distinct(),
                    expressionValues.stream().filter(expr -> !isDeterministic(expr)))
                .collect(toImmutableList());

        if (simplifiedExpressionValues.size() == 1) {
          return new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              toExpression(value, type),
              simplifiedExpressionValues.get(0));
        }

        return new InPredicate(
            toExpression(value, type), new InListExpression(simplifiedExpressionValues));
      }
      if (hasNullValue) {
        return null;
      }
      return false;
    }

    @Override
    protected Object visitArithmeticUnary(ArithmeticUnaryExpression node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);
      if (value == null) {
        return null;
      }
      if (value instanceof Expression) {
        Expression valueExpression = toExpression(value, type(node.getValue()));
        switch (node.getSign()) {
          case PLUS:
            return valueExpression;
          case MINUS:
            if (valueExpression instanceof ArithmeticUnaryExpression
                && ((ArithmeticUnaryExpression) valueExpression).getSign().equals(MINUS)) {
              return ((ArithmeticUnaryExpression) valueExpression).getValue();
            }
            return new ArithmeticUnaryExpression(MINUS, valueExpression);
        }
      }

      if (node.getSign() == PLUS) {
        return value;
      } else {
        try {
          Expression valueExpression = toExpression(value, type(node.getValue()));
          if (valueExpression instanceof ArithmeticUnaryExpression
              && ((ArithmeticUnaryExpression) valueExpression).getSign().equals(MINUS)) {
            return ((ArithmeticUnaryExpression) valueExpression).getValue();
          }
          return new ArithmeticUnaryExpression(MINUS, valueExpression);
          // TODO use the following after we implement InterpretedFunctionInvoker
          //          return invokeOperator(OperatorType.NEGATION, types(node.getValue()),
          // Collections.singletonList(value));
        } catch (Throwable throwable) {
          throwIfInstanceOf(throwable, RuntimeException.class);
          throwIfInstanceOf(throwable, Error.class);
          throw new RuntimeException(throwable.getMessage(), throwable);
        }
      }
    }

    @Override
    protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context) {
      Object left = processWithExceptionHandling(node.getLeft(), context);
      if (left == null) {
        return null;
      }
      Object right = processWithExceptionHandling(node.getRight(), context);
      if (right == null) {
        return null;
      }

      if (hasUnresolvedValue(left, right)) {
        return new ArithmeticBinaryExpression(
            node.getOperator(),
            toExpression(left, type(node.getLeft())),
            toExpression(right, type(node.getRight())));
      } else {
        return new ArithmeticBinaryExpression(
            node.getOperator(),
            toExpression(left, type(node.getLeft())),
            toExpression(right, type(node.getRight())));
        // TODO use the following after we implement InterpretedFunctionInvoker
        //        return invokeOperator(OperatorType.valueOf(node.getOperator().name()),
        // types(node.getLeft(), node.getRight()),
        //            ImmutableList.of(left, right));
      }
    }

    @Override
    protected Object visitComparisonExpression(ComparisonExpression node, Object context) {
      ComparisonExpression.Operator operator = node.getOperator();
      Expression left = node.getLeft();
      Expression right = node.getRight();

      if (operator == ComparisonExpression.Operator.IS_DISTINCT_FROM) {
        return processIsDistinctFrom(context, left, right);
      }
      // Execution engine does not have not equal and greater than operators, so interpret with
      // equal or less than, but do not flip operator in result, as many optimizers depend on
      // operators not flipping
      if (node.getOperator() == ComparisonExpression.Operator.NOT_EQUAL) {
        Object result = visitComparisonExpression(flipComparison(node), context);
        if (result == null) {
          return null;
        }
        if (result instanceof ComparisonExpression) {
          return flipComparison((ComparisonExpression) result);
        }
        return !(Boolean) result;
      }
      if (node.getOperator() == ComparisonExpression.Operator.GREATER_THAN
          || node.getOperator() == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL) {
        Object result = visitComparisonExpression(flipComparison(node), context);
        if (result instanceof ComparisonExpression) {
          return flipComparison((ComparisonExpression) result);
        }
        return result;
      }

      return processComparisonExpression(context, operator, left, right);
    }

    private Object processIsDistinctFrom(
        Object context, Expression leftExpression, Expression rightExpression) {
      Object left = processWithExceptionHandling(leftExpression, context);
      Object right = processWithExceptionHandling(rightExpression, context);

      if (left == null && right instanceof Expression) {
        return new IsNotNullPredicate((Expression) right);
      }

      if (right == null && left instanceof Expression) {
        return new IsNotNullPredicate((Expression) left);
      }

      if (left instanceof Expression || right instanceof Expression) {
        return new ComparisonExpression(
            ComparisonExpression.Operator.IS_DISTINCT_FROM,
            toExpression(left, type(leftExpression)),
            toExpression(right, type(rightExpression)));
      }

      return invokeOperator(
          OperatorType.valueOf(ComparisonExpression.Operator.IS_DISTINCT_FROM.name()),
          types(leftExpression, rightExpression),
          Arrays.asList(left, right));
    }

    private Object processComparisonExpression(
        Object context,
        ComparisonExpression.Operator operator,
        Expression leftExpression,
        Expression rightExpression) {
      Object left = processWithExceptionHandling(leftExpression, context);
      if (left == null) {
        return null;
      }

      Object right = processWithExceptionHandling(rightExpression, context);
      if (right == null) {
        return null;
      }

      if (left instanceof Expression || right instanceof Expression) {
        return new ComparisonExpression(
            operator,
            toExpression(left, type(leftExpression)),
            toExpression(right, type(rightExpression)));
      } else {
        return new ComparisonExpression(
            operator,
            toExpression(left, type(leftExpression)),
            toExpression(right, type(rightExpression)));
        // TODO use the following after we implement InterpretedFunctionInvoker
        //        return invokeOperator(OperatorType.valueOf(operator.name()), types(leftExpression,
        // rightExpression),
        //            ImmutableList.of(left, right));
      }
    }

    // TODO define method contract or split into separate methods, as flip(EQUAL) is a negation,
    // while flip(LESS_THAN) is just flipping sides
    private ComparisonExpression flipComparison(ComparisonExpression comparisonExpression) {
      switch (comparisonExpression.getOperator()) {
        case EQUAL:
          return new ComparisonExpression(
              ComparisonExpression.Operator.NOT_EQUAL,
              comparisonExpression.getLeft(),
              comparisonExpression.getRight());
        case NOT_EQUAL:
          return new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              comparisonExpression.getLeft(),
              comparisonExpression.getRight());
        case LESS_THAN:
          return new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN,
              comparisonExpression.getRight(),
              comparisonExpression.getLeft());
        case LESS_THAN_OR_EQUAL:
          return new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
              comparisonExpression.getRight(),
              comparisonExpression.getLeft());
        case GREATER_THAN:
          return new ComparisonExpression(
              ComparisonExpression.Operator.LESS_THAN,
              comparisonExpression.getRight(),
              comparisonExpression.getLeft());
        case GREATER_THAN_OR_EQUAL:
          return new ComparisonExpression(
              ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
              comparisonExpression.getRight(),
              comparisonExpression.getLeft());
        default:
          throw new IllegalStateException(
              "Unexpected value: " + comparisonExpression.getOperator());
      }
    }

    @Override
    protected Object visitBetweenPredicate(BetweenPredicate node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);
      if (value == null) {
        return null;
      }
      Object min = processWithExceptionHandling(node.getMin(), context);
      Object max = processWithExceptionHandling(node.getMax(), context);

      if (value instanceof Expression || min instanceof Expression || max instanceof Expression) {
        return new BetweenPredicate(
            toExpression(value, type(node.getValue())),
            toExpression(min, type(node.getMin())),
            toExpression(max, type(node.getMax())));
      } else {
        return new BetweenPredicate(
            toExpression(value, type(node.getValue())),
            toExpression(min, type(node.getMin())),
            toExpression(max, type(node.getMax())));
        // TODO use the following after we implement InterpretedFunctionInvoker
        //        Boolean greaterOrEqualToMin = null;
        //        if (min != null) {
        //          greaterOrEqualToMin =
        //              (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL,
        // types(node.getMin(), node.getValue()),
        //                  ImmutableList.of(min, value));
        //        }
        //        Boolean lessThanOrEqualToMax = null;
        //        if (max != null) {
        //          lessThanOrEqualToMax =
        //              (Boolean) invokeOperator(OperatorType.LESS_THAN_OR_EQUAL,
        // types(node.getValue(), node.getMax()),
        //                  ImmutableList.of(value, max));
        //        }
        //
        //        if (greaterOrEqualToMin == null) {
        //          return Objects.equals(lessThanOrEqualToMax, Boolean.FALSE) ? false : null;
        //        }
        //        if (lessThanOrEqualToMax == null) {
        //          return Objects.equals(greaterOrEqualToMin, Boolean.FALSE) ? false : null;
        //        }
        //        return greaterOrEqualToMin && lessThanOrEqualToMax;
      }
    }

    @Override
    protected Object visitNotExpression(NotExpression node, Object context) {
      Object value = processWithExceptionHandling(node.getValue(), context);
      if (value == null) {
        return null;
      }

      if (value instanceof Expression) {
        return new NotExpression(toExpression(value, type(node.getValue())));
      }

      return !(Boolean) value;
    }

    @Override
    protected Object visitLogicalExpression(LogicalExpression node, Object context) {
      List<Object> terms = new ArrayList<>();
      List<Type> types = new ArrayList<>();

      for (Expression term : node.getTerms()) {
        Object processed = processWithExceptionHandling(term, context);

        switch (node.getOperator()) {
          case AND:
            if (Boolean.FALSE.equals(processed)) {
              return false;
            }
            if (!Boolean.TRUE.equals(processed)) {
              terms.add(processed);
              types.add(type(term));
            }
            break;
          case OR:
            if (Boolean.TRUE.equals(processed)) {
              return true;
            }
            if (!Boolean.FALSE.equals(processed)) {
              terms.add(processed);
              types.add(type(term));
            }
        }
      }

      if (terms.isEmpty()) {
        switch (node.getOperator()) {
          case AND:
            return true; // terms are true
          case OR:
            return false; // all terms are false
        }
      }

      if (terms.size() == 1) {
        return terms.get(0);
      }

      if (terms.stream().allMatch(Objects::isNull)) {
        return null;
      }

      ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
      for (int i = 0; i < terms.size(); i++) {
        expressions.add(toExpression(terms.get(i), types.get(i)));
      }
      return new LogicalExpression(node.getOperator(), expressions.build());
    }

    @Override
    protected Object visitBooleanLiteral(BooleanLiteral node, Object context) {
      return node.equals(BooleanLiteral.TRUE_LITERAL);
    }

    @Override
    protected Object visitFunctionCall(FunctionCall node, Object context) {
      List<Type> argumentTypes = new ArrayList<>();
      List<Object> argumentValues = new ArrayList<>();
      for (Expression expr : node.getArguments()) {
        Object value = processWithExceptionHandling(expr, context);
        Type type = type(expr);
        argumentValues.add(value);
        argumentTypes.add(type);
      }

      if (optimize && hasUnresolvedValue(argumentValues)) {
        verify(!node.isDistinct(), "distinct not supported");
        //        verify(node.getOrderBy().isEmpty(), "order by not supported");
        //        verify(node.getFilter().isEmpty(), "filter not supported");
        //        verify(node.getWindow().isEmpty(), "window not supported");
        return new FunctionCall(
            node.getName(), node.isDistinct(), toExpressions(argumentValues, argumentTypes));
      } else {
        return new FunctionCall(
            node.getName(), node.isDistinct(), toExpressions(argumentValues, argumentTypes));

        // TODO use the following after we implement InterpretedFunctionInvoker
        //        return
        // functionInvoker.invoke(TableBuiltinScalarFunction.valueOf(node.getName().getSuffix()),
        // session, argumentTypes, argumentValues);
      }
    }

    @Override
    public Object visitCast(Cast node, Object context) {
      Object value = processWithExceptionHandling(node.getExpression(), context);
      Type targetType = plannerContext.getTypeManager().getType(toTypeSignature(node.getType()));
      Type sourceType = type(node.getExpression());
      if (value instanceof Expression) {
        if (targetType.equals(sourceType)) {
          return value;
        }

        return new Cast((Expression) value, node.getType(), node.isSafe());
      }

      if (value == null) {
        return null;
      }

      try {
        return CastFunctionHelper.cast(value, sourceType, targetType, session);
      } catch (RuntimeException e) {
        if (node.isSafe()) {
          return null;
        }
        throw e;
      }
    }

    @Override
    protected Object visitExpression(Expression node, Object context) {
      throw new SemanticException("not yet implemented: " + node.getClass().getName());
    }

    private List<Type> types(Expression... expressions) {
      return Stream.of(expressions)
          .map(NodeRef::of)
          .map(expressionTypes::get)
          .collect(toImmutableList());
    }

    private List<Type> types(List<Expression> expressions) {
      return expressions.stream()
          .map(NodeRef::of)
          .map(expressionTypes::get)
          .collect(toImmutableList());
    }

    private boolean hasUnresolvedValue(Object... values) {
      return hasUnresolvedValue(ImmutableList.copyOf(values));
    }

    private boolean hasUnresolvedValue(List<Object> values) {
      return values.stream().anyMatch(instanceOf(Expression.class));
    }

    private Object invokeOperator(
        OperatorType operatorType,
        List<? extends Type> argumentTypes,
        List<Object> argumentValues) {
      return functionInvoker.invoke(operatorType, session, argumentTypes, argumentValues);
    }

    private Expression toExpression(Object base, Type type) {
      return literalEncoder.toExpression(base, type);
    }

    private List<Expression> toExpressions(List<Object> values, List<Type> types) {
      return literalEncoder.toExpressions(values, types);
    }
  }
}
