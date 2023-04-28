package org.apache.iotdb.db.metadata.view.viewExpression.binary.logic;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LogicOrViewExpression extends LogicBinaryViewExpression {

  public LogicOrViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public LogicOrViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public LogicOrViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLogicOrExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "OR";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LOGIC_OR;
  }
}
