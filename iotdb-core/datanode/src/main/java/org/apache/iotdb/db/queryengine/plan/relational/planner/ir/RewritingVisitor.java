package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.Cast;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter.sameElements;

public class RewritingVisitor<C> extends IrVisitor<Expression, C> {

  @Override
  protected Expression visitExpression(Expression node, C context) {
    // RewritingVisitor must have explicit support for each expression type, with a dedicated
    // visit method,
    // so visitExpression() should never be called.
    throw new UnsupportedOperationException(
        "visit() not implemented for " + node.getClass().getName());
  }

  @Override
  protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    Expression left = process(node.getLeft(), context);
    Expression right = process(node.getRight(), context);

    if (left != node.getLeft() || right != node.getRight()) {
      // node.getLocation().get() may be null
      return new ArithmeticBinaryExpression(
          node.getLocation().get(), node.getOperator(), left, right);
    }

    return node;
  }

  @Override
  protected Expression visitComparisonExpression(ComparisonExpression node, C context) {
    Expression left = process(node.getLeft(), context);
    Expression right = process(node.getRight(), context);

    if (left != node.getLeft() || right != node.getRight()) {
      return new ComparisonExpression(node.getOperator(), left, right);
    }

    return node;
  }

  @Override
  protected Expression visitBetweenPredicate(BetweenPredicate node, C context) {
    Expression value = process(node.getValue(), context);
    Expression min = process(node.getMin(), context);
    Expression max = process(node.getMax(), context);

    if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
      return new BetweenPredicate(value, min, max);
    }

    return node;
  }

  @Override
  protected Expression visitLogicalExpression(LogicalExpression node, C context) {
    List<Expression> terms =
        node.getTerms().stream().map(term -> process(term, context)).collect(Collectors.toList());
    if (!sameElements(node.getTerms(), terms)) {
      return new LogicalExpression(node.getOperator(), terms);
    }

    return node;
  }

  @Override
  protected Expression visitNotExpression(NotExpression node, C context) {
    Expression value = process(node.getValue(), context);

    if (value != node.getValue()) {
      return new NotExpression(value);
    }

    return node;
  }

  @Override
  protected Expression visitLikePredicate(LikePredicate node, C context) {
    Expression value = process(node.getValue(), context);
    Expression pattern = process(node.getPattern(), context);
    Expression escape =
        node.getEscape().isPresent() ? process(node.getEscape().get(), context) : null;

    if (value != node.getValue()
        || pattern != node.getPattern()
        || (escape != null && escape != node.getEscape().get())) {
      return new LikePredicate(value, pattern, escape);
    }

    return node;
  }

  @Override
  protected Expression visitIsNullPredicate(IsNullPredicate node, C context) {
    Expression value = process(node.getValue(), context);

    if (value != node.getValue()) {
      return new IsNullPredicate(value);
    }

    return node;
  }

  @Override
  protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    Expression value = process(node.getValue(), context);

    if (value != node.getValue()) {
      return new IsNotNullPredicate(value);
    }

    return node;
  }

  @Override
  protected Expression visitNullIfExpression(NullIfExpression node, C context) {
    Expression first = process(node.getFirst(), context);
    Expression second = process(node.getSecond(), context);

    if (first != node.getFirst() || second != node.getSecond()) {
      return new NullIfExpression(first, second);
    }

    return node;
  }

  //    @Override
  //    protected Expression visitSearchedCaseExpression(
  //            SearchedCaseExpression node, C context) {
  //        ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
  //        for (WhenClause expression : node.getWhenClauses()) {
  //            builder.add(process(expression, context));
  //        }
  //
  //        Optional<Expression> defaultValue =
  //                node.getDefaultValue().map(value -> rewrite(value, context.get()));
  //
  //        if (!sameElements(node.getDefaultValue(), defaultValue)
  //                || !sameElements(node.getWhenClauses(), builder.build())) {
  //            // defaultValue.get() may be null
  //            return new SearchedCaseExpression(builder.build(), defaultValue.get());
  //        }
  //
  //        return node;
  //    }

  @Override
  protected Expression visitFunctionCall(FunctionCall node, C context) {
    List<Expression> arguments =
        node.getArguments().stream()
            .map(argument -> process(argument, context))
            .collect(Collectors.toList());

    if (!sameElements(node.getArguments(), arguments)) {
      return new FunctionCall(node.getName(), arguments);
    }
    return node;
  }

  @Override
  protected Expression visitInPredicate(InPredicate node, C context) {
    Expression value = process(node.getValue(), context);
    //            List<Expression> values = node.getValueList().stream()
    //                    .map(entry -> rewrite(entry, context.get()))
    //                    .collect(toImmutableList());

    if (node.getValue() != value
        || !sameElements(Optional.of(value), Optional.of(node.getValue()))) {
      return new InPredicate(value, value);
    }

    return node;
  }

  @Override
  protected Expression visitCast(Cast node, C context) {
    Expression expression = process(node.getExpression(), context);

    if (expression != node.getExpression()) {
      return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
    }

    return node;
  }

  @Override
  protected Expression visitSymbolReference(SymbolReference node, C context) {
    return node;
  }

  @Override
  protected Expression visitIdentifier(Identifier node, C context) {
    return node;
  }

  @Override
  protected Expression visitLiteral(Literal node, C context) {
    return node;
  }
}
