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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Map;

/** Like {@link org.apache.tsfile.utils.ReadWriteIOUtils}. */
public class WALWriteUtils {
  public static final int BOOLEAN_LEN = ReadWriteIOUtils.BOOLEAN_LEN;
  public static final int SHORT_LEN = ReadWriteIOUtils.SHORT_LEN;
  public static final int INT_LEN = ReadWriteIOUtils.INT_LEN;
  public static final int LONG_LEN = ReadWriteIOUtils.LONG_LEN;
  public static final int DOUBLE_LEN = ReadWriteIOUtils.DOUBLE_LEN;
  public static final int FLOAT_LEN = ReadWriteIOUtils.FLOAT_LEN;

  private static final int NO_BYTE_TO_READ = -1;

  private WALWriteUtils() {}

  /** Write a byte to byteBuffer according to flag. If flag is true, write 1, else write 0. */
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
   * Write a byte n to byteBuffer.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   *     form.
   */
  public static int write(byte n, IWALByteBufferView buffer) {
    buffer.put(n);
    return Byte.BYTES;
  }

  /**
   * Write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, IWALByteBufferView buffer) {
    buffer.putShort(n);
    return SHORT_LEN;
  }

  /**
   * Write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(Binary n, IWALByteBufferView buffer) {
    buffer.putInt(n.getLength());
    buffer.put(n.getValues());
    return INT_LEN + n.getLength();
  }

  /**
   * Write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, IWALByteBufferView buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /** Write a long n to byteBuffer. */
  public static int write(long n, IWALByteBufferView buffer) {
    buffer.putLong(n);
    return LONG_LEN;
  }

  /** Write a float n to byteBuffer. */
  public static int write(float n, IWALByteBufferView buffer) {
    buffer.putFloat(n);
    return FLOAT_LEN;
  }

  /** Write a double n to byteBuffer. */
  public static int write(double n, IWALByteBufferView buffer) {
    buffer.putDouble(n);
    return DOUBLE_LEN;
  }

  /**
   * Write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, IWALByteBufferView buffer) {
    if (s == null) {
      return write(NO_BYTE_TO_READ, buffer);
    }
    int len = 0;
    byte[] bytes = s.getBytes();
    len += write(bytes.length, buffer);
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * Write IDeviceID to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(IDeviceID deviceID, IWALByteBufferView buffer) {
    if (deviceID == null) {
      return write(NO_BYTE_TO_READ, buffer);
    }
    int len = 0;
    len += write(deviceID.segmentNum(), buffer);
    for (int i = 0; i < deviceID.segmentNum(); i++) {
      String segment = (String) deviceID.segment(i);
      len += write(segment, buffer);
    }
    return len;
  }

  /** TSDataType. */
  public static int write(TSDataType dataType, IWALByteBufferView buffer) {
    byte n = dataType.serialize();
    return write(n, buffer);
  }

  /** TSEncoding. */
  public static int write(TSEncoding encoding, IWALByteBufferView buffer) {
    byte n = encoding.serialize();
    return write(n, buffer);
  }

  /** CompressionType. */
  public static int write(CompressionType compressionType, IWALByteBufferView buffer) {
    byte n = compressionType.serialize();
    return write(n, buffer);
  }

  /** MeasurementSchema. */
  public static int write(MeasurementSchema measurementSchema, IWALByteBufferView buffer) {
    int len = 0;

    len += write(measurementSchema.getMeasurementName(), buffer);

    len += write(measurementSchema.getType(), buffer);

    len += write(measurementSchema.getEncodingType(), buffer);

    len += write(measurementSchema.getCompressor(), buffer);

    Map<String, String> props = measurementSchema.getProps();
    if (props == null) {
      len += write(0, buffer);
    } else {
      len += write(props.size(), buffer);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        len += write(entry.getKey(), buffer);
        len += write(entry.getValue(), buffer);
      }
    }
    return len;
  }

  public static int sizeToWrite(MeasurementSchema measurementSchema) {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.sizeToWrite(measurementSchema.getMeasurementName());
    byteLen += 3 * Byte.BYTES;

    Map<String, String> props = measurementSchema.getProps();
    byteLen += Integer.BYTES;
    if (props != null) {
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.sizeToWrite(entry.getKey());
        byteLen += ReadWriteIOUtils.sizeToWrite(entry.getValue());
      }
    }

    return byteLen;
  }
}
