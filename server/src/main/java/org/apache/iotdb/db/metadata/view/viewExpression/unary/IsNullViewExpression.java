package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IsNullViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final boolean isNot;

  public IsNullViewExpression(ViewExpression expression, boolean isNot) {
    super(expression);
    this.isNot = isNot;
  }

  public IsNullViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public IsNullViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      isNot = ReadWriteIOUtils.readBool(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitIsNullExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.IS_NULL;
  }

  @Override
  public String toString(boolean isRoot) {
    return this.expression.toString(false) + " IS_NULL";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isNot, stream);
  }
  // endregion

  public boolean isNot() {
    return isNot;
  }
}
