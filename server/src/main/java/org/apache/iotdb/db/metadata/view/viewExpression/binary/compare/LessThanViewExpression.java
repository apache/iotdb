package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LessThanViewExpression extends CompareBinaryViewExpression {

  public LessThanViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public LessThanViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public LessThanViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLessThanExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "<";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LESS_THAN;
  }
}
