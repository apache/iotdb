package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.binary.BinaryViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class CompareBinaryViewExpression extends BinaryViewExpression {

  // region member variables and init functions
  protected CompareBinaryViewExpression(
      ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  protected CompareBinaryViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  protected CompareBinaryViewExpression(InputStream inputStream) {
    super(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitCompareBinaryExpression(this, context);
  }
  // endregion
}
