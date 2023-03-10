package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;

import java.nio.ByteBuffer;

public class WhenThenExpression extends BinaryExpression {

  public WhenThenExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public WhenThenExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public void setWhen(Expression expression) {
    leftExpression = expression;
  }

  public void setThen(Expression expression) {
    rightExpression = expression;
  }

  public Expression getWhen() {
    return leftExpression;
  }

  public Expression getThen() {
    return rightExpression;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.WHEN_THEN;
  }

  @Override
  protected String operator() {
    return "When Then";
  }

  @Override
  public String getExpressionStringInternal() {
    return "WHEN " + this.getWhen().toString() + " THEN " + this.getThen().toString();
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitWhenThenExpression(this, context);
  }
}
