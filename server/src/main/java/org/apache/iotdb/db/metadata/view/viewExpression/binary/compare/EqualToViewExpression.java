package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class EqualToViewExpression extends CompareBinaryViewExpression {

  public EqualToViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public EqualToViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public EqualToViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitEqualToExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "==";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.EQUAL_TO;
  }
}
