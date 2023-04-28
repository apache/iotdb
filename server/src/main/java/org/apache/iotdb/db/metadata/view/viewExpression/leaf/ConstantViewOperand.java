package org.apache.iotdb.db.metadata.view.viewExpression.leaf;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ConstantViewOperand extends LeafViewOperand {

  // region member variables and init functions
  private final String valueString;
  private final TSDataType dataType;

  public ConstantViewOperand(TSDataType dataType, String valueString) {
    this.dataType = Validate.notNull(dataType);
    this.valueString = Validate.notNull(valueString);
  }

  public ConstantViewOperand(ByteBuffer byteBuffer) {
    dataType = TSDataType.deserializeFrom(byteBuffer);
    valueString = ReadWriteIOUtils.readString(byteBuffer);
  }

  public ConstantViewOperand(InputStream inputStream) {
    try {
      dataType = TSDataType.deserialize(ReadWriteIOUtils.readByte(inputStream));
      valueString = ReadWriteIOUtils.readString(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitConstantOperand(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.CONSTANT;
  }

  @Override
  public String toString(boolean isRoot) {
    return this.valueString;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    dataType.serializeTo(byteBuffer);
    ReadWriteIOUtils.write(valueString, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    stream.write(dataType.serialize());
    ReadWriteIOUtils.write(valueString, stream);
  }

  // endregion

  public TSDataType getDataType() {
    return dataType;
  }

  public String getValueString() {
    return valueString;
  }
}
