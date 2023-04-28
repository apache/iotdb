package org.apache.iotdb.db.metadata.view.viewExpression.leaf;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TimeSeriesViewOperand extends LeafViewOperand {

  // region member variables and init functions
  private String pathString;

  public TimeSeriesViewOperand(String path) {
    this.pathString = path;
  }

  public TimeSeriesViewOperand(ByteBuffer byteBuffer) {
    this.pathString = ReadWriteIOUtils.readString(byteBuffer);
  }

  public TimeSeriesViewOperand(InputStream inputStream) {
    try {
      this.pathString = ReadWriteIOUtils.readString(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTimeSeriesOperand(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.TIMESERIES;
  }

  @Override
  public String toString(boolean isRoot) {
    return this.pathString;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(pathString, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(pathString, stream);
  }
  // endregion

  public String getPathString() {
    return pathString;
  }

  public void setPathString(String path) {
    this.pathString = path;
  }
}
