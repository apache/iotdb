package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class NonEqualViewExpression extends CompareBinaryViewExpression {

  public NonEqualViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public NonEqualViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public NonEqualViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitNonEqualExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "!=";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.NON_EQUAL;
  }
}
