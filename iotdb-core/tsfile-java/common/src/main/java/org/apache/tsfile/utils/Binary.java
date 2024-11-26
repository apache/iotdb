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
package org.apache.tsfile.utils;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOf;

/**
 * Override compareTo() and equals() function to Binary class. This class is used to accept Java
 * String type
 */
public class Binary implements Comparable<Binary>, Serializable, Accountable {

  private static final long INSTANCE_SIZE = shallowSizeOfInstance(Binary.class);
  private static final long serialVersionUID = 6394197743397020735L;
  public static final Binary EMPTY_VALUE = new Binary(new byte[0]);

  private byte[] values;

  /** if the bytes v is modified, the modification is visible to this binary. */
  public Binary(byte[] v) {
    this.values = v;
  }

  public Binary(String s, Charset charset) {
    this.values = (s == null) ? null : s.getBytes(charset);
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

    // copied from StringLatin1.compareT0
    int len1 = getLength();
    int len2 = other.getLength();
    int lim = Math.min(len1, len2);
    for (int k = 0; k < lim; k++) {
      if (this.values[k] != other.values[k]) {
        return getChar(values, k) - getChar(other.values, k);
      }
    }
    return len1 - len2;
  }

  // avoid overflow
  private char getChar(byte[] val, int index) {
    return (char) (val[index] & 0xff);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Binary binary = (Binary) o;
    return Arrays.equals(values, binary.values);
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

  public String getStringValue(Charset charset) {
    return new String(this.values, charset);
  }

  @Override
  public String toString() {
    // use UTF_8 by default since toString do not provide parameter
    return getStringValue(StandardCharsets.UTF_8);
  }

  public byte[] getValues() {
    return values;
  }

  public void setValues(byte[] values) {
    this.values = values;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + sizeOf(values);
  }
}
