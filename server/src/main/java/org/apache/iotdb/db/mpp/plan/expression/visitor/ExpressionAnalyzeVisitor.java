package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;

public abstract class ExpressionAnalyzeVisitor<R, C> extends ExpressionVisitor<R, C> {
  @Override
  public R visitExpression(Expression expression, C context) {
    throw new IllegalArgumentException(
        "unsupported expression type: " + expression.getExpressionType());
  }
}
