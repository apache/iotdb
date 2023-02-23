package org.apache.iotdb.pipe.api.type;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Override compareTo() and equals() function to Binary class. This class is used to accept Java
 * String type
 */
public class Binary implements Comparable<Binary>, Serializable {

  private static final long serialVersionUID = 1250049718612917815L;
  public static final String STRING_ENCODING = "UTF-8";
  public static final Charset STRING_CHARSET = Charset.forName(STRING_ENCODING);

  private final byte[] values;

  private int hash;

  // indicate whether hash has been calculated
  private boolean hasCalculatedHash;

  private String stringCache;

  /** if the bytes v is modified, the modification is visible to this binary. */
  public Binary(byte[] v) {
    this.values = v;
  }

  public Binary(String s) {
    this.values = (s == null) ? null : s.getBytes(STRING_CHARSET);
  }

  public static Binary valueOf(String value) {
    return new Binary(stringToBytes(value));
  }

  @Override
  public int compareTo(Binary other) {
    if (other == null) {
      if (this.values == null) {
        return 0;
      } else {
        return 1;
      }
    }

    int i = 0;
    while (i < getLength() && i < other.getLength()) {
      if (this.values[i] == other.values[i]) {
        i++;
        continue;
      }
      return this.values[i] - other.values[i];
    }
    return getLength() - other.getLength();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    return compareTo((Binary) other) == 0;
  }

  @Override
  public int hashCode() {
    if (!hasCalculatedHash) {
      hash = Arrays.hashCode(values);
      hasCalculatedHash = true;
    }
    return hash;
  }

  /**
   * Get length of values. Returns -1 if values is null.
   *
   * @return length
   */
  public int getLength() {
    if (this.values == null) {
      return -1;
    }
    return this.values.length;
  }

  public boolean isNull() {
    return values == null;
  }

  public String getStringValue() {
    if (values == null) {
      return null;
    }
    if (stringCache == null) {
      stringCache = new String(this.values, STRING_CHARSET);
    }
    return stringCache;
  }

  public String getTextEncodingType() {
    return STRING_ENCODING;
  }

  @Override
  public String toString() {
    return getStringValue();
  }

  public byte[] getValues() {
    return values;
  }

  /**
   * convert string to byte array using UTF-8 encoding.
   *
   * @param str input string
   * @return byte array
   */
  public static byte[] stringToBytes(String str) {
    return str.getBytes(STRING_CHARSET);
  }
}
