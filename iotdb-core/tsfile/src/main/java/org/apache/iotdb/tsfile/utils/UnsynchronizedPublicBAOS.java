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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * This class extends ByteArrayOutputStream and intentionally remove the 'synchronized' keyword in
 * write methods for better performance. (Not thread safe)
 */
public class UnsynchronizedPublicBAOS extends PublicBAOS {
  private void ensureCapacity(int minCapacity) {
    // overflow-conscious code
    if (minCapacity - buf.length > 0) grow(minCapacity);
  }

  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = buf.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0) newCapacity = minCapacity;
    if (newCapacity - MAX_ARRAY_SIZE > 0) newCapacity = hugeCapacity(minCapacity);
    buf = Arrays.copyOf(buf, newCapacity);
  }

  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) // overflow
    throw new OutOfMemoryError();
    return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
  }

  @Override
  public void write(int b) {
    ensureCapacity(count + 1);
    buf[count] = (byte) b;
    count += 1;
  }

  @Override
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(count + len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOf(buf, count);
  }

  @Override
  public String toString() {
    return new String(buf, 0, count);
  }

  @Override
  public String toString(String charsetName) throws UnsupportedEncodingException {
    return new String(buf, 0, count, charsetName);
  }
}
