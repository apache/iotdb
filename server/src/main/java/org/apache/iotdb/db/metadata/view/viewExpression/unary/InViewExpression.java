package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;

public class InViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final boolean isNotIn;

  private final List<String> valueList;

  public InViewExpression(ViewExpression expression, boolean isNotIn, List<String> values) {
    super(expression);
    this.isNotIn = isNotIn;
    this.valueList = values;
  }

  public InViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    isNotIn = ReadWriteIOUtils.readBool(byteBuffer);
    valueList = ReadWriteIOUtils.readStringList(byteBuffer);
  }

  public InViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      isNotIn = ReadWriteIOUtils.readBool(inputStream);
      valueList = ReadWriteIOUtils.readStringList(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitInExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.IN;
  }

  @Override
  public String toString(boolean isRoot) {
    return "IN " + this.expression.toString();
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNotIn, byteBuffer);
    ReadWriteIOUtils.writeStringList(this.valueList, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(isNotIn, stream);
    ReadWriteIOUtils.writeStringList(this.valueList, stream);
  }
  // endregion

  public boolean isNotIn() {
    return isNotIn;
  }

  public LinkedHashSet<String> getValuesInLinkedHashSet() {
    return new LinkedHashSet<>(this.valueList);
  }

  public List<String> getValueList() {
    return this.valueList;
  }
}
