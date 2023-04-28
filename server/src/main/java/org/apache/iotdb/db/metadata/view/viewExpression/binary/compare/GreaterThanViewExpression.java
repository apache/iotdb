package org.apache.iotdb.db.metadata.view.viewExpression.binary.compare;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class GreaterThanViewExpression extends CompareBinaryViewExpression {

  public GreaterThanViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public GreaterThanViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public GreaterThanViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitGreaterThanExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return ">";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.GREATER_THAN;
  }
}
