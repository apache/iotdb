package org.apache.iotdb.db.metadata.view.viewExpression.leaf;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TimestampViewOperand extends LeafViewOperand {
  // region member variables and init functions
  public TimestampViewOperand() {
    // do nothing
  };

  public TimestampViewOperand(ByteBuffer byteBuffer) {
    // do nothing
  }

  public TimestampViewOperand(InputStream inputStream) {
    // do nothing
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTimeStampOperand(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.TIMESTAMP;
  }

  @Override
  public String toString(boolean isRoot) {
    return "TIMESTAMP";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    // do nothing
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    // do nothing
  }

  // endregion
}
