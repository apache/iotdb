package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;

public abstract class ExpressionAnalyzeVisitor<R> extends ExpressionVisitor<R, Void> {
  @Override
  public R visitExpression(Expression expression, Void context) {
    throw new IllegalArgumentException(
        "unsupported expression type: " + expression.getExpressionType());
  }
}
