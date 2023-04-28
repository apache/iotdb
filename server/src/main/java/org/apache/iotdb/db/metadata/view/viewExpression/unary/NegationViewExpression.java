package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class NegationViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  public NegationViewExpression(ViewExpression expression) {
    super(expression);
  }

  public NegationViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
  }

  public NegationViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitNegationExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.NEGATION;
  }

  @Override
  public String toString(boolean isRoot) {
    return "NEGATION_OF " + this.expression.toString(false);
  }
  // endregion
}
