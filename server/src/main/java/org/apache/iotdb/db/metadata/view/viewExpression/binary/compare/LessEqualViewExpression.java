package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LessEqualViewExpression extends CompareBinaryViewExpression {

  public LessEqualViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public LessEqualViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public LessEqualViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLessEqualExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "<=";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LESS_EQUAL;
  }
}
