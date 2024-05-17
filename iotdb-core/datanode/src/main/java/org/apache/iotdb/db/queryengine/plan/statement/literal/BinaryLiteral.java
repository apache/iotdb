package org.apache.iotdb.db.queryengine.plan.statement.literal;

import org.apache.iotdb.db.exception.sql.SemanticException;

import com.google.common.base.CharMatcher;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BinaryLiteral extends Literal {
  // the grammar could possibly include whitespace in the value it passes to us
  private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
  private static final CharMatcher HEX_DIGIT_MATCHER =
      CharMatcher.inRange('A', 'F').or(CharMatcher.inRange('0', '9')).precomputed();

  private final byte[] values;

  public BinaryLiteral(String value) {
    requireNonNull(value, "value is null");
    if (value.length() < 3 || !value.startsWith("X'") || !value.endsWith("'")) {
      throw new SemanticException("Binary literal must be in the form X'hexstring'");
    }
    value = value.substring(2, value.length() - 1);
    String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
    if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
      throw new SemanticException("Binary literal can only contain hexadecimal digits");
    }
    if (hexString.length() % 2 != 0) {
      throw new SemanticException("Binary literal must contain an even number of digits");
    }
    int len = hexString.length();
    this.values = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      this.values[i / 2] =
          (byte)
              ((Character.digit(hexString.charAt(i), 16) << 4)
                  + Character.digit(hexString.charAt(i + 1), 16));
    }
  }

  public byte[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(LiteralType.BINARY.ordinal(), byteBuffer);
    for (byte b : values) {
      ReadWriteIOUtils.write(b, byteBuffer);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(LiteralType.BINARY.ordinal(), stream);
    for (byte b : values) {
      ReadWriteIOUtils.write(b, stream);
    }
  }

  @Override
  public boolean isDataTypeConsistency(TSDataType dataType) {
    return dataType == TSDataType.BLOB;
  }

  @Override
  public String getDataTypeString() {
    return TSDataType.BLOB.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryLiteral that = (BinaryLiteral) o;
    return Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }
}
