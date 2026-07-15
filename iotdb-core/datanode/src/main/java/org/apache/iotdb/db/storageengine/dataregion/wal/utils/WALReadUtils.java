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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** Read methods paired with {@link WALWriteUtils}. */
public class WALReadUtils {

  private WALReadUtils() {}

  public static String readString(DataInputStream stream) throws IOException {
    int strLength = stream.readInt();
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    stream.readFully(bytes);
    return new String(bytes, TSFileConfig.STRING_CHARSET);
  }

  public static String readString(ByteBuffer buffer) {
    int strLength = buffer.getInt();
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes);
    return new String(bytes, TSFileConfig.STRING_CHARSET);
  }

  public static MeasurementSchema readMeasurementSchema(DataInputStream stream) throws IOException {
    String measurementId = readString(stream);
    byte type = stream.readByte();
    byte encoding = stream.readByte();
    byte compressor = stream.readByte();
    Map<String, String> props = readProps(stream);
    return new MeasurementSchema(measurementId, type, encoding, compressor, props);
  }

  public static MeasurementSchema readMeasurementSchema(ByteBuffer buffer) {
    String measurementId = readString(buffer);
    byte type = buffer.get();
    byte encoding = buffer.get();
    byte compressor = buffer.get();
    Map<String, String> props = readProps(buffer);
    return new MeasurementSchema(measurementId, type, encoding, compressor, props);
  }

  private static Map<String, String> readProps(DataInputStream stream) throws IOException {
    int size = stream.readInt();
    if (size <= 0) {
      return null;
    }
    Map<String, String> props = new HashMap<>();
    for (int i = 0; i < size; i++) {
      props.put(readString(stream), readString(stream));
    }
    return props;
  }

  private static Map<String, String> readProps(ByteBuffer buffer) {
    int size = buffer.getInt();
    if (size <= 0) {
      return null;
    }
    Map<String, String> props = new HashMap<>();
    for (int i = 0; i < size; i++) {
      props.put(readString(buffer), readString(buffer));
    }
    return props;
  }
}
