package org.apache.iotdb.db.metadata.view.viewExpression.leaf;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class NullViewOperand extends LeafViewOperand {

  // region member variables and init functions
  public NullViewOperand() {};
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitNullOperand(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.NULL;
  }

  @Override
  public String toString(boolean isRoot) {
    return "null";
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
