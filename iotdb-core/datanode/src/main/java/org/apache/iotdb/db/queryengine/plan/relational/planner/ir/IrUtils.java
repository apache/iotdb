/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrExpressionInterpreter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.NoOpSymbolResolver;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.FALSE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public final class IrUtils {
  private IrUtils() {}

  public static List<Expression> extractConjuncts(Expression expression) {
    return extractPredicates(LogicalExpression.Operator.AND, expression);
  }

  public static List<Expression> extractDisjuncts(Expression expression) {
    return extractPredicates(LogicalExpression.Operator.OR, expression);
  }

  public static List<Expression> extractPredicates(LogicalExpression expression) {
    return extractPredicates(expression.getOperator(), expression);
  }

  // Use for table device fetching
  // Expand the inPredicates to better check the in list and hit device cache
  public static List<Expression> extractOrPredicatesWithInExpanded(final Expression expression) {
    ImmutableList.Builder<Expression> resultBuilder = ImmutableList.builder();
    extractOrPredicatesWithInExpanded(expression, resultBuilder);
    return resultBuilder.build();
  }

  private static void extractOrPredicatesWithInExpanded(
      final Expression expression, final ImmutableList.Builder<Expression> resultBuilder) {
    if (expression instanceof LogicalExpression) {
      if (((LogicalExpression) expression).getOperator() == LogicalExpression.Operator.OR) {
        for (final Expression term : ((LogicalExpression) expression).getTerms()) {
          extractOrPredicatesWithInExpanded(term, resultBuilder);
        }
      }
    } else if (expression instanceof InPredicate) {
      ((InListExpression) ((InPredicate) expression).getValueList())
          .getValues().stream()
              .map(
                  value ->
                      new ComparisonExpression(
                          ComparisonExpression.Operator.EQUAL,
                          ((InPredicate) expression).getValue(),
                          value))
              .forEach(resultBuilder::add);
    } else {
      resultBuilder.add(expression);
    }
  }

  public static List<Expression> extractPredicates(
      LogicalExpression.Operator operator, Expression expression) {
    ImmutableList.Builder<Expression> resultBuilder = ImmutableList.builder();
    extractPredicates(operator, expression, resultBuilder);
    return resultBuilder.build();
  }

  private static void extractPredicates(
      LogicalExpression.Operator operator,
      Expression expression,
      ImmutableList.Builder<Expression> resultBuilder) {
    if (expression instanceof LogicalExpression
        && ((LogicalExpression) expression).getOperator() == operator) {
      for (Expression term : ((LogicalExpression) expression).getTerms()) {
        extractPredicates(operator, term, resultBuilder);
      }
    } else {
      resultBuilder.add(expression);
    }
  }

  public static Expression and(Expression... expressions) {
    return and(Arrays.asList(expressions));
  }

  public static Expression and(Collection<Expression> expressions) {
    return logicalExpression(LogicalExpression.Operator.AND, expressions);
  }

  public static Expression or(Expression... expressions) {
    return or(Arrays.asList(expressions));
  }

  public static Expression or(Collection<Expression> expressions) {
    return logicalExpression(LogicalExpression.Operator.OR, expressions);
  }

  public static Expression logicalExpression(
      LogicalExpression.Operator operator, Collection<Expression> expressions) {
    requireNonNull(operator, "operator is null");
    requireNonNull(expressions, "expressions is null");

    if (expressions.isEmpty()) {
      switch (operator) {
        case AND:
          return TRUE_LITERAL;
        case OR:
          return FALSE_LITERAL;
      }
      throw new IllegalArgumentException("Unsupported LogicalExpression operator");
    }

    if (expressions.size() == 1) {
      return Iterables.getOnlyElement(expressions);
    }

    return new LogicalExpression(operator, ImmutableList.copyOf(expressions));
  }

  public static Expression combinePredicates(
      LogicalExpression.Operator operator, Collection<Expression> expressions) {
    if (operator == LogicalExpression.Operator.AND) {
      return combineConjuncts(expressions);
    }

    return combineDisjuncts(expressions);
  }

  public static Expression combineConjuncts(Expression... expressions) {
    return combineConjuncts(Arrays.asList(expressions));
  }

  public static Expression filterDeterministicConjuncts(Expression expression) {
    return filterConjuncts(expression, DeterminismEvaluator::isDeterministic);
  }

  public static Expression combineConjuncts(Collection<Expression> expressions) {
    requireNonNull(expressions, "expressions is null");

    List<Expression> conjuncts =
        expressions.stream()
            .flatMap(e -> extractConjuncts(e).stream())
            .filter(e -> !e.equals(TRUE_LITERAL))
            .collect(toList());

    conjuncts = removeDuplicates(conjuncts);

    if (conjuncts.contains(FALSE_LITERAL)) {
      return FALSE_LITERAL;
    }

    return and(conjuncts);
  }

  public static Expression combineConjunctsWithDuplicates(Collection<Expression> expressions) {
    requireNonNull(expressions, "expressions is null");

    List<Expression> conjuncts =
        expressions.stream()
            .flatMap(e -> extractConjuncts(e).stream())
            .filter(e -> !e.equals(TRUE_LITERAL))
            .collect(toList());

    if (conjuncts.contains(FALSE_LITERAL)) {
      return FALSE_LITERAL;
    }

    return and(conjuncts);
  }

  public static Expression combineDisjuncts(Expression... expressions) {
    return combineDisjuncts(Arrays.asList(expressions));
  }

  public static Expression combineDisjuncts(Collection<Expression> expressions) {
    return combineDisjunctsWithDefault(expressions, FALSE_LITERAL);
  }

  public static Expression combineDisjunctsWithDefault(
      Collection<Expression> expressions, Expression emptyDefault) {
    requireNonNull(expressions, "expressions is null");

    List<Expression> disjuncts =
        expressions.stream()
            .flatMap(e -> extractDisjuncts(e).stream())
            .filter(e -> !e.equals(FALSE_LITERAL))
            .collect(toList());

    disjuncts = removeDuplicates(disjuncts);

    if (disjuncts.contains(TRUE_LITERAL)) {
      return TRUE_LITERAL;
    }

    return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
  }

  //    public static Expression filterDeterministicConjuncts(Metadata metadata, Expression
  // expression)
  //    {
  //        return filterConjuncts(expression, expression1 ->
  // DeterminismEvaluator.isDeterministic(expression1));
  //    }
  //
  //    public static Expression filterNonDeterministicConjuncts(Metadata metadata, Expression
  // expression)
  //    {
  //        return filterConjuncts(expression, not(testExpression ->
  // DeterminismEvaluator.isDeterministic(testExpression)));
  //    }

  public static Expression filterConjuncts(Expression expression, Predicate<Expression> predicate) {
    List<Expression> conjuncts =
        extractConjuncts(expression).stream().filter(predicate).collect(toList());

    return combineConjuncts(conjuncts);
  }

  public static boolean isEffectivelyLiteral(
      Expression expression, PlannerContext plannerContext, SessionInfo session) {
    if (expression instanceof Literal) {
      return true;
    }
    if (expression instanceof Cast) {
      return ((Cast) expression).getExpression() instanceof Literal
          // a Cast(Literal(...)) can fail, so this requires verification
          && constantExpressionEvaluatesSuccessfully(plannerContext, session, expression);
    }
    if (expression instanceof FunctionCall) {
      String functionName = ((FunctionCall) expression).getName().getSuffix();
      if (functionName.equals("pi") || functionName.equals("e")) {
        return true;
      }
    }

    return false;
  }

  private static boolean constantExpressionEvaluatesSuccessfully(
      PlannerContext plannerContext, SessionInfo session, Expression constantExpression) {
    Map<NodeRef<Expression>, Type> types =
        new IrTypeAnalyzer(plannerContext)
            .getTypes(session, TypeProvider.empty(), constantExpression);
    IrExpressionInterpreter interpreter =
        new IrExpressionInterpreter(constantExpression, plannerContext, session, types);
    Object literalValue = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
    return !(literalValue instanceof Expression);
  }

  //    @SafeVarargs
  //    public static Function<Expression, Expression> expressionOrNullSymbols(Predicate<Symbol>...
  // nullSymbolScopes)
  //    {
  //        return expression -> {
  //            ImmutableList.Builder<Expression> resultDisjunct = ImmutableList.builder();
  //            resultDisjunct.add(expression);
  //
  //            for (Predicate<Symbol> nullSymbolScope : nullSymbolScopes) {
  //                List<Symbol> symbols = SymbolsExtractor.extractUnique(expression).stream()
  //                        .filter(nullSymbolScope)
  //                        .collect(toImmutableList());
  //
  //                if (symbols.isEmpty()) {
  //                    continue;
  //                }
  //
  //                ImmutableList.Builder<Expression> nullConjuncts = ImmutableList.builder();
  //                for (Symbol symbol : symbols) {
  //                    nullConjuncts.add(new IsNullPredicate(symbol.toSymbolReference()));
  //                }
  //
  //                resultDisjunct.add(and(nullConjuncts.build()));
  //            }
  //
  //            return or(resultDisjunct.build());
  //        };
  //    }

  /**
   * Removes duplicate deterministic expressions. Preserves the relative order of the expressions in
   * the list.
   */
  private static List<Expression> removeDuplicates(List<Expression> expressions) {
    Set<Expression> seen = new HashSet<>();

    ImmutableList.Builder<Expression> result = ImmutableList.builder();
    for (Expression expression : expressions) {
      if (!DeterminismEvaluator.isDeterministic(expression)) {
        result.add(expression);
      } else if (!seen.contains(expression)) {
        result.add(expression);
        seen.add(expression);
      }
    }

    return result.build();
  }

  //  public static Stream<Expression> preOrder(Expression node) {
  //    return stream(
  //        Traverser.forTree((SuccessorsFunction<Expression>) Expression::getChildren)
  //            .depthFirstPreOrder(requireNonNull(node, "node is null")));
  //  }
}
