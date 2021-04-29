/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Override compareTo() and equals() function to Binary class. This class is used to accept Java
 * String type
 */
public class Binary implements Comparable<Binary>, Serializable {

  private static final long serialVersionUID = 6394197743397020735L;
  public static final Binary EMPTY_VALUE = new Binary("");

  private byte[] values;

  /** if the bytes v is modified, the modification is visible to this binary. */
  public Binary(byte[] v) {
    this.values = v;
  }

  public Binary(String s) {
    this.values = (s == null) ? null : s.getBytes(TSFileConfig.STRING_CHARSET);
  }

  public static Binary valueOf(String value) {
    return new Binary(BytesUtils.stringToBytes(value));
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
    return Arrays.hashCode(values);
  }

  /**
   * get length.
   *
   * @return length
   */
  public int getLength() {
    if (this.values == null) {
      return -1;
    }
    return this.values.length;
  }

  public String getStringValue() {
    return new String(this.values, TSFileConfig.STRING_CHARSET);
  }

  public String getTextEncodingType() {
    return TSFileConfig.STRING_ENCODING;
  }

  @Override
  public String toString() {
    return getStringValue();
  }

  public byte[] getValues() {
    return values;
  }

  public void setValues(byte[] values) {
    this.values = values;
  }
}
