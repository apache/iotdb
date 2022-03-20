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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/** Like {@link org.apache.iotdb.tsfile.utils.ReadWriteIOUtils} */
public class WALWriteUtils {
  public static final int BOOLEAN_LEN = ReadWriteIOUtils.BOOLEAN_LEN;
  public static final int SHORT_LEN = ReadWriteIOUtils.SHORT_LEN;
  public static final int INT_LEN = ReadWriteIOUtils.INT_LEN;
  public static final int LONG_LEN = ReadWriteIOUtils.LONG_LEN;
  public static final int DOUBLE_LEN = ReadWriteIOUtils.DOUBLE_LEN;
  public static final int FLOAT_LEN = ReadWriteIOUtils.FLOAT_LEN;

  private WALWriteUtils() {}

  /** write a byte to byteBuffer according to flag. If flag is true, write 1, else write 0. */
  public static int write(Boolean flag, IWALByteBufferView buffer) {
    byte a;
    if (Boolean.TRUE.equals(flag)) {
      a = 1;
    } else {
      a = 0;
    }

    buffer.put(a);
    return BOOLEAN_LEN;
  }

  /**
   * write a byte n to byteBuffer.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, IWALByteBufferView buffer) {
    buffer.put(n);
    return Byte.BYTES;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, IWALByteBufferView buffer) {
    buffer.putShort(n);
    return SHORT_LEN;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(Binary n, IWALByteBufferView buffer) {
    buffer.putInt(n.getLength());
    buffer.put(n.getValues());
    return INT_LEN + n.getLength();
  }

  /**
   * write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, IWALByteBufferView buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /** write a long n to byteBuffer. */
  public static int write(long n, IWALByteBufferView buffer) {
    buffer.putLong(n);
    return LONG_LEN;
  }

  /** write a float n to byteBuffer. */
  public static int write(float n, IWALByteBufferView buffer) {
    buffer.putFloat(n);
    return FLOAT_LEN;
  }

  /** write a double n to byteBuffer. */
  public static int write(double n, IWALByteBufferView buffer) {
    buffer.putDouble(n);
    return DOUBLE_LEN;
  }

  /**
   * write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, IWALByteBufferView buffer) {
    if (s == null) {
      return write(-1, buffer);
    }
    int len = 0;
    byte[] bytes = s.getBytes();
    len += write(bytes.length, buffer);
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /** TSDataType. */
  public static int write(TSDataType dataType, IWALByteBufferView buffer) {
    byte n = dataType.serialize();
    return write(n, buffer);
  }
}
