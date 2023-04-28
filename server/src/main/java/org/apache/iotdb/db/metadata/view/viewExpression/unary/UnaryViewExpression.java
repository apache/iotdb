package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class UnaryViewExpression extends ViewExpression {

  // region member variables and init functions
  protected final ViewExpression expression;

  protected UnaryViewExpression(ViewExpression expression) {
    this.expression = expression;
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitUnaryExpression(this, context);
  }

  @Override
  protected final boolean isLeafOperandInternal() {
    return false;
  }

  @Override
  public final List<ViewExpression> getChildViewExpressions() {
    // leaf node has no child nodes.
    return Arrays.asList(expression);
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ViewExpression.serialize(expression, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ViewExpression.serialize(expression, stream);
  }
  // endregion

  public final ViewExpression getExpression() {
    return expression;
  }
}
