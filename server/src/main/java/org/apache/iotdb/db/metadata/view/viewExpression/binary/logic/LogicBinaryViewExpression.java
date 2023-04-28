package org.apache.iotdb.db.metadata.view.viewExpression.binary.logic;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.binary.BinaryViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class LogicBinaryViewExpression extends BinaryViewExpression {

  // region member variables and init functions
  protected LogicBinaryViewExpression(
      ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  protected LogicBinaryViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  protected LogicBinaryViewExpression(InputStream inputStream) {
    super(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLogicBinaryExpression(this, context);
  }
  // endregion
}
