package org.apache.iotdb.db.metadata.view.viewExpression.binary.arithmetic;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class AdditionViewExpression extends ArithmeticBinaryViewExpression {

  // region member variables and init functions
  public AdditionViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public AdditionViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public AdditionViewExpression(InputStream inputStream) {
    super(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitAdditionExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "+";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.ADDITION;
  }
  // endregion
}
