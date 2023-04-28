package org.apache.iotdb.db.metadata.view.viewExpression.unary;

import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class LikeViewExpression extends UnaryViewExpression {

  // region member variables and init functions
  private final String patternString;
  private final Pattern pattern;

  public LikeViewExpression(ViewExpression expression, String patternString) {
    super(expression);
    this.patternString = patternString;
    pattern = this.compile();
  }

  public LikeViewExpression(ViewExpression expression, String patternString, Pattern pattern) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
  }

  public LikeViewExpression(ByteBuffer byteBuffer) {
    super(ViewExpression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    pattern = compile();
  }

  public LikeViewExpression(InputStream inputStream) {
    super(ViewExpression.deserialize(inputStream));
    try {
      patternString = ReadWriteIOUtils.readString(inputStream);
      pattern = compile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLikeExpression(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.LIKE;
  }

  @Override
  public String toString(boolean isRoot) {
    String basicString = this.expression.toString(false) + "LIKE " + this.patternString;
    if (isRoot) {
      return basicString;
    }
    return "( " + basicString + " )";
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

  /**
   * This Method is for un-escaping strings except '\' before special string '%', '_', '\', because
   * we need to use '\' to judge whether to replace this to regexp string
   */
  private String unescapeString(String value) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      String ch = String.valueOf(value.charAt(i));
      if ("\\".equals(ch)) {
        if (i < value.length() - 1) {
          String nextChar = String.valueOf(value.charAt(i + 1));
          if ("%".equals(nextChar) || "_".equals(nextChar) || "\\".equals(nextChar)) {
            stringBuilder.append(ch);
          }
          if ("\\".equals(nextChar)) {
            i++;
          }
        }
      } else {
        stringBuilder.append(ch);
      }
    }
    return stringBuilder.toString();
  }

  /**
   * The main idea of this part comes from
   * https://codereview.stackexchange.com/questions/36861/convert-sql-like-to-regex/36864
   */
  private Pattern compile() {
    String unescapeValue = unescapeString(patternString);
    String specialRegexString = ".^$*+?{}[]|()";
    StringBuilder patternBuilder = new StringBuilder();
    patternBuilder.append("^");
    for (int i = 0; i < unescapeValue.length(); i++) {
      String ch = String.valueOf(unescapeValue.charAt(i));
      if (specialRegexString.contains(ch)) {
        ch = "\\" + unescapeValue.charAt(i);
      }
      if (i == 0
          || !"\\".equals(String.valueOf(unescapeValue.charAt(i - 1)))
          || i >= 2
              && "\\\\"
                  .equals(
                      patternBuilder.substring(
                          patternBuilder.length() - 2, patternBuilder.length()))) {
        patternBuilder.append(ch.replace("%", ".*?").replace("_", "."));
      } else {
        patternBuilder.append(ch);
      }
    }
    patternBuilder.append("$");
    return Pattern.compile(patternBuilder.toString());
  }
}
