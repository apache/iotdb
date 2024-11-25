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
package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataTypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public final class ExpressionTreeRewriter<C> {
  private final ExpressionRewriter<C> rewriter;
  private final AstVisitor<Expression, Context<C>> visitor;

  public ExpressionTreeRewriter(ExpressionRewriter<C> rewriter) {
    this.rewriter = rewriter;
    this.visitor = new RewritingVisitor();
  }

  public static <T extends Expression> T rewriteWith(ExpressionRewriter<Void> rewriter, T node) {
    return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
  }

  public static <C, T extends Expression> T rewriteWith(
      ExpressionRewriter<C> rewriter, T node, C context) {
    return new ExpressionTreeRewriter<>(rewriter).rewrite(node, context);
  }

  private List<Expression> rewrite(List<Expression> items, Context<C> context) {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    for (Expression expression : items) {
      builder.add(rewrite(expression, context.get()));
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  public <T extends Expression> T rewrite(T node, C context) {
    return (T) visitor.process(node, new Context<>(context, false));
  }

  /**
   * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the
   * expression rewriter for the provided node.
   */
  @SuppressWarnings("unchecked")
  public <T extends Expression> T defaultRewrite(T node, C context) {
    return (T) visitor.process(node, new Context<>(context, true));
  }

  private class RewritingVisitor extends AstVisitor<Expression, Context<C>> {

    @Override
    protected Expression visitExpression(Expression node, Context<C> context) {
      // RewritingVisitor must have explicit support for each expression type, with a dedicated
      // visit method,
      // so visitExpression() should never be called.
      throw new UnsupportedOperationException(
          "visit() not implemented for " + node.getClass().getName());
    }

    @Override
    protected Expression visitRow(Row node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result = rewriter.rewriteRow(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> items = rewrite(node.getItems(), context);

      if (!sameElements(node.getItems(), items)) {
        return new Row(items);
      }

      return node;
    }

    @Override
    protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteArithmeticUnary(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression child = rewrite(node.getValue(), context.get());
      if (child != node.getValue()) {
        return new ArithmeticUnaryExpression(node.getSign(), child);
      }

      return node;
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteArithmeticBinary(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression left = rewrite(node.getLeft(), context.get());
      Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ArithmeticBinaryExpression(node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    public Expression visitComparisonExpression(ComparisonExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteComparisonExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression left = rewrite(node.getLeft(), context.get());
      Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ComparisonExpression(node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    protected Expression visitBetweenPredicate(BetweenPredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteBetweenPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression min = rewrite(node.getMin(), context.get());
      Expression max = rewrite(node.getMax(), context.get());

      if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
        return new BetweenPredicate(value, min, max);
      }

      return node;
    }

    @Override
    public Expression visitLogicalExpression(LogicalExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteLogicalExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> terms = rewrite(node.getTerms(), context);
      if (!sameElements(node.getTerms(), terms)) {
        return new LogicalExpression(node.getOperator(), terms);
      }

      return node;
    }

    @Override
    public Expression visitNotExpression(NotExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteNotExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new NotExpression(value);
      }

      return node;
    }

    @Override
    protected Expression visitIsNullPredicate(IsNullPredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteIsNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNullPredicate(value);
      }

      return node;
    }

    @Override
    protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteIsNotNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNotNullPredicate(value);
      }

      return node;
    }

    @Override
    protected Expression visitNullIfExpression(NullIfExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteNullIfExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression first = rewrite(node.getFirst(), context.get());
      Expression second = rewrite(node.getSecond(), context.get());

      if (first != node.getFirst() || second != node.getSecond()) {
        return new NullIfExpression(first, second);
      }

      return node;
    }

    @Override
    protected Expression visitIfExpression(IfExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteIfExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression condition = rewrite(node.getCondition(), context.get());
      Expression trueValue = rewrite(node.getTrueValue(), context.get());
      Expression falseValue = null;
      if (node.getFalseValue().isPresent()) {
        falseValue = rewrite(node.getFalseValue().get(), context.get());
      }

      if ((condition != node.getCondition())
          || (trueValue != node.getTrueValue())
          || (falseValue != node.getFalseValue().orElse(null))) {
        return new IfExpression(condition, trueValue, falseValue);
      }

      return node;
    }

    @Override
    protected Expression visitSearchedCaseExpression(
        SearchedCaseExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteSearchedCaseExpression(
                node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (WhenClause expression : node.getWhenClauses()) {
        builder.add(rewrite(expression, context.get()));
      }

      Optional<Expression> defaultValue =
          node.getDefaultValue().map(value -> rewrite(value, context.get()));

      if (!sameElements(node.getDefaultValue(), defaultValue)
          || !sameElements(node.getWhenClauses(), builder.build())) {
        return defaultValue
            .map(expression -> new SearchedCaseExpression(builder.build(), expression))
            .orElseGet(() -> new SearchedCaseExpression(builder.build()));
      }

      return node;
    }

    @Override
    protected Expression visitSimpleCaseExpression(SimpleCaseExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteSimpleCaseExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression operand = rewrite(node.getOperand(), context.get());

      ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (WhenClause expression : node.getWhenClauses()) {
        builder.add(rewrite(expression, context.get()));
      }

      Optional<Expression> defaultValue =
          node.getDefaultValue().map(value -> rewrite(value, context.get()));

      if (operand != node.getOperand()
          || !sameElements(node.getDefaultValue(), defaultValue)
          || !sameElements(node.getWhenClauses(), builder.build())) {
        return defaultValue
            .map(expression -> new SimpleCaseExpression(operand, builder.build(), expression))
            .orElseGet(() -> new SimpleCaseExpression(operand, builder.build()));
      }

      return node;
    }

    @Override
    protected Expression visitWhenClause(WhenClause node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteWhenClause(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression operand = rewrite(node.getOperand(), context.get());
      Expression result = rewrite(node.getResult(), context.get());

      if (operand != node.getOperand() || result != node.getResult()) {
        return new WhenClause(operand, result);
      }
      return node;
    }

    @Override
    protected Expression visitCoalesceExpression(CoalesceExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteCoalesceExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> operands = rewrite(node.getOperands(), context);

      if (!sameElements(node.getOperands(), operands)) {
        return new CoalesceExpression(operands);
      }

      return node;
    }

    @Override
    public Expression visitFunctionCall(FunctionCall node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteFunctionCall(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> arguments = rewrite(node.getArguments(), context);

      if (!sameElements(node.getArguments(), arguments)) {
        return new FunctionCall(node.getName(), arguments);
      }
      return node;
    }

    @Override
    public Expression visitLikePredicate(LikePredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteLikePredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression pattern = rewrite(node.getPattern(), context.get());
      Optional<Expression> rewrittenEscape =
          node.getEscape().map(escape -> rewrite(escape, context.get()));

      if (value != node.getValue()
          || pattern != node.getPattern()
          || !sameElements(node.getEscape(), rewrittenEscape)) {
        return rewrittenEscape
            .map(expression -> new LikePredicate(value, pattern, expression))
            .orElseGet(() -> new LikePredicate(value, pattern));
      }

      return node;
    }

    @Override
    public Expression visitInPredicate(InPredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteInPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression list = rewrite(node.getValueList(), context.get());

      if (node.getValue() != value || node.getValueList() != list) {
        return new InPredicate(value, list);
      }

      return node;
    }

    //        @Override
    //        public Expression visitConstant(Constant node, Context<C> context)
    //        {
    //            if (!context.isDefaultRewrite()) {
    //                Expression result = rewriter.rewriteConstant(node, context.get(),
    // ExpressionTreeRewriter.this);
    //                if (result != null) {
    //                    return result;
    //                }
    //            }
    //
    //            return node;
    //        }

    @Override
    protected Expression visitInListExpression(InListExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteInListExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> values = rewrite(node.getValues(), context);

      if (!sameElements(node.getValues(), values)) {
        return new InListExpression(values);
      }

      return node;
    }

    @Override
    protected Expression visitLiteral(final Literal node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression result =
            rewriter.rewriteLiteral(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitParameter(Parameter node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteParameter(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitIdentifier(Identifier node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteIdentifier(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitDereferenceExpression(DereferenceExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteDereferenceExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression base = rewrite(node.getBase(), context.get());
      if (base != node.getBase()) {
        if (node.getField().isPresent()) {
          return new DereferenceExpression(base, node.getField().get());
        }
        return new DereferenceExpression((Identifier) base);
      }

      return node;
    }

    @Override
    public Expression visitCast(Cast node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result = rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression expression = rewrite(node.getExpression(), context.get());
      DataType type = rewrite(node.getType(), context.get());

      if (node.getExpression() != expression || node.getType() != type) {
        return new Cast(expression, type, node.isSafe());
      }

      return node;
    }

    @Override
    public Expression visitCurrentDatabase(final CurrentDatabase node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression result =
            rewriter.rewriteCurrentDatabase(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitCurrentUser(final CurrentUser node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression result =
            rewriter.rewriteCurrentUser(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    protected Expression visitGenericDataType(GenericDataType node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteGenericDataType(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Identifier name = rewrite(node.getName(), context.get());

      ImmutableList.Builder<DataTypeParameter> arguments = ImmutableList.builder();
      for (DataTypeParameter argument : node.getArguments()) {
        if (argument instanceof NumericParameter) {
          arguments.add(argument);
        } else if (argument instanceof TypeParameter) {
          TypeParameter parameter = (TypeParameter) argument;
          DataType value = (DataType) process(parameter.getValue(), context);

          if (value != parameter.getValue()) {
            arguments.add(new TypeParameter(value));
          } else {
            arguments.add(argument);
          }
        }
      }

      List<DataTypeParameter> rewrittenArguments = arguments.build();

      if (name != node.getName() || !sameElements(rewrittenArguments, node.getArguments())) {
        return node.getLocation().isPresent()
            ? new GenericDataType(node.getLocation().get(), name, rewrittenArguments)
            : new GenericDataType(name, rewrittenArguments);
      }

      return node;
    }

    @Override
    protected Expression visitFieldReference(FieldReference node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteFieldReference(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    protected Expression visitSymbolReference(SymbolReference node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteSymbolReference(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    protected Expression visitQuantifiedComparisonExpression(
        QuantifiedComparisonExpression node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteQuantifiedComparison(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression subquery = rewrite(node.getSubquery(), context.get());

      if (node.getValue() != value || node.getSubquery() != subquery) {
        return new QuantifiedComparisonExpression(
            node.getOperator(), node.getQuantifier(), value, subquery);
      }

      return node;
    }

    @Override
    protected Expression visitTrim(Trim node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result = rewriter.rewriteTrim(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
      expressions.add(node.getTrimSource());
      node.getTrimCharacter().ifPresent(expressions::add);

      Expression trimSource = rewrite(node.getTrimSource(), context.get());
      Optional<Expression> trimChar =
          node.getTrimCharacter().isPresent()
              ? Optional.of(rewrite(node.getTrimCharacter().get(), context.get()))
              : Optional.empty();

      if (trimSource != node.getTrimSource() || !sameElements(trimChar, node.getTrimCharacter())) {
        return trimChar
            .map(expression -> new Trim(node.getSpecification(), trimSource, expression))
            .orElseGet(() -> new Trim(node.getSpecification(), trimSource));
      }

      return node;
    }
  }

  public static class Context<C> {
    private final boolean defaultRewrite;
    private final C context;

    private Context(C context, boolean defaultRewrite) {
      this.context = context;
      this.defaultRewrite = defaultRewrite;
    }

    public C get() {
      return context;
    }

    public boolean isDefaultRewrite() {
      return defaultRewrite;
    }
  }

  public static <T> boolean sameElements(Optional<T> a, Optional<T> b) {
    if (!a.isPresent() && !b.isPresent()) {
      return true;
    }
    if (a.isPresent() != b.isPresent()) {
      return false;
    }

    return a.get() == b.get();
  }

  @SuppressWarnings("ObjectEquality")
  public static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b) {
    if (Iterables.size(a) != Iterables.size(b)) {
      return false;
    }

    Iterator<? extends T> first = a.iterator();
    Iterator<? extends T> second = b.iterator();

    while (first.hasNext() && second.hasNext()) {
      if (first.next() != second.next()) {
        return false;
      }
    }

    return true;
  }
}
