package org.apache.iotdb.db.metadata.view.viewExpression.binary.logic;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class LogicAndViewExpression extends LogicBinaryViewExpression {

  public LogicAndViewExpression(ViewExpression leftExpression, ViewExpression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public LogicAndViewExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public LogicAndViewExpression(InputStream inputStream) {
    super(inputStream);
  }

  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLogicAndExpression(this, context);
  }

  @Override
  public String getStringSymbol() {
    return "AND";
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LOGIC_AND;
  }
}
