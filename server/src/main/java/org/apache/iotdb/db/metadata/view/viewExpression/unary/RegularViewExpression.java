package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class RegularViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final String patternString;
  private final Pattern pattern;

  public RegularViewExpression(ViewExpression expression, String patternString) {
    super(expression);
    this.patternString = patternString;
    pattern = Pattern.compile(patternString);
  }

  public RegularViewExpression(ViewExpression expression, String patternString, Pattern pattern) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
  }

  public RegularViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    pattern = Pattern.compile(Validate.notNull(patternString));
  }

  public RegularViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      patternString = ReadWriteIOUtils.readString(inputStream);
      pattern = Pattern.compile(Validate.notNull(patternString));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitRegularExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.REGEXP;
  }

  @Override
  public String toString(boolean isRoot) {
    return "REGULAR(" + this.expression.toString() + ", " + this.patternString + ")";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
  }
  // endregion
  public String getPatternString() {
    return patternString;
  }

  public Pattern getPattern() {
    return pattern;
  }
}
