package org.apache.iotdb.db.metadata.view.viewExpression.binary.arithmetic;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class MultiplicationViewExpression extends ArithmeticBinaryViewExpression {

  // region member variables and init functions
  public MultiplicationViewExpression(
      ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public MultiplicationViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public MultiplicationViewExpression(InputStream inputStream) {
    super(inputStream);
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitMultiplicationExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "*";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.MULTIPLICATION;
  }
  // endregion

}
