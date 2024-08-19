package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.stream.Stream;

/**
 * Extracts and returns the stream of all expression subtrees within an Expression, including
 * Expression itself
 */
public final class SubExpressionExtractor {
  private SubExpressionExtractor() {}

  public static Stream<Expression> extract(Expression expression) {
    return AstUtils.preOrder(expression)
        .filter(Expression.class::isInstance)
        .map(Expression.class::cast);
  }
}
