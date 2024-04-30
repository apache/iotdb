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

import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.Cast;
import org.apache.iotdb.db.relational.sql.tree.CoalesceExpression;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.Row;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;
import org.apache.iotdb.db.relational.sql.tree.WhenClause;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public final class ExpressionTreeRewriter<C> {
  private final ExpressionRewriter<C> rewriter;
  private final IrVisitor<Expression, Context<C>> visitor;

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

  private class RewritingVisitor extends IrVisitor<Expression, Context<C>> {

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

    //        @Override
    //        protected Expression visitArithmeticNegation(ArithmeticNegation node, Context<C>
    // context)
    //        {
    //            if (!context.isDefaultRewrite()) {
    //                Expression result = rewriter.rewriteArithmeticUnary(node, context.get(),
    // ExpressionTreeRewriter.this);
    //                if (result != null) {
    //                    return result;
    //                }
    //            }
    //
    //            Expression child = rewrite(node.getValue(), context.get());
    //            if (child != node.getValue()) {
    //                return new ArithmeticNegation(child);
    //            }
    //
    //            return node;
    //        }

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
        // node.getLocation().get() may be null
        return new ArithmeticBinaryExpression(
            node.getLocation().get(), node.getOperator(), left, right);
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
        builder.add(rewriteWhenClause(expression, context));
      }

      Optional<Expression> defaultValue =
          node.getDefaultValue().map(value -> rewrite(value, context.get()));

      if (!sameElements(node.getDefaultValue(), defaultValue)
          || !sameElements(node.getWhenClauses(), builder.build())) {
        // defaultValue.get() may be null
        return new SearchedCaseExpression(builder.build(), defaultValue.get());
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
        builder.add(rewriteWhenClause(expression, context));
      }

      Optional<Expression> defaultValue =
          node.getDefaultValue().map(value -> rewrite(value, context.get()));

      if (operand != node.getOperand()
          || !sameElements(node.getDefaultValue(), defaultValue)
          || !sameElements(node.getWhenClauses(), builder.build())) {
        // defaultValue.get() may be null
        return new SimpleCaseExpression(operand, builder.build(), defaultValue.get());
      }

      return node;
    }

    protected WhenClause rewriteWhenClause(WhenClause node, Context<C> context) {
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
    public Expression visitInPredicate(InPredicate node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result =
            rewriter.rewriteInPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      //            List<Expression> values = node.getValueList().stream()
      //                    .map(entry -> rewrite(entry, context.get()))
      //                    .collect(toImmutableList());

      if (node.getValue() != value
          || !sameElements(Optional.of(value), Optional.of(node.getValue()))) {
        return new InPredicate(value, value);
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
    public Expression visitCast(Cast node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Expression result = rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression expression = rewrite(node.getExpression(), context.get());

      if (node.getExpression() != expression) {
        return new Cast(expression, node.getType(), node.isSafe());
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
    protected Expression visitIdentifier(Identifier node, Context<C> context) {
      return node;
    }

    @Override
    protected Expression visitLiteral(Literal node, Context<C> context) {
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
