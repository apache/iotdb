package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LogicNotViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  public LogicNotViewExpression(ViewExpression expression) {
    super(expression);
  }

  public LogicNotViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
  }

  public LogicNotViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLogicNotExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LOGIC_NOT;
  }

  @Override
  public String toString(boolean isRoot) {
    return "NOT " + this.expression.toString(false);
  }
  // endregion
}
