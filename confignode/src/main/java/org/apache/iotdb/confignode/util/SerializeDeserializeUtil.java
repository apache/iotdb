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

package org.apache.iotdb.confignode.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializeDeserializeUtil {
  public static final int INT_LEN = 4;

  private SerializeDeserializeUtil() {}

  /** read string from byteBuffer. */
  public static String readString(ByteBuffer buffer) {
    int strLength = readInt(buffer);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return "";
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return new String(bytes, 0, strLength);
  }

  /** read a int var from byteBuffer. */
  public static int readInt(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /**
   * write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, ByteBuffer buffer) {
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

  /**
   * write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, ByteBuffer buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /**
   * write a map to buffer
   *
   * @param map Map<String, String>
   * @param buffer
   * @return length
   */
  public static int write(Map<String, String> map, ByteBuffer buffer) {
    if (map == null) {
      return write(-1, buffer);
    }

    int length = 0;
    byte[] bytes;
    buffer.putInt(map.size());
    length += 4;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      bytes = entry.getKey().getBytes();
      buffer.putInt(bytes.length);
      length += 4;
      buffer.put(bytes);
      length += bytes.length;
      bytes = entry.getValue().getBytes();
      buffer.putInt(bytes.length);
      length += 4;
      buffer.put(bytes);
      length += bytes.length;
    }
    return length;
  }

  /**
   * read map from buffer
   *
   * @param buffer ByteBuffer
   * @return Map<String, String>
   */
  public static Map<String, String> readMap(ByteBuffer buffer) {
    int length = readInt(buffer);
    if (length == -1) {
      return null;
    }
    Map<String, String> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(buffer);
      // value
      String value = readString(buffer);
      map.put(key, value);
    }
    return map;
  }

  /**
   * write a string map to buffer
   *
   * @param map Map<String, String>
   * @param buffer
   * @return length
   */
  public static int writeStringMapLists(Map<String, List<String>> map, ByteBuffer buffer) {
    if (map == null) {
      return write(-1, buffer);
    }

    int length = 0;
    byte[] bytes;
    buffer.putInt(map.size());
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      bytes = entry.getKey().getBytes();
      buffer.putInt(bytes.length);
      buffer.put(bytes);
      buffer.putInt(entry.getValue().size());
      entry
          .getValue()
          .forEach(
              b -> {
                buffer.putInt(b.length());
                buffer.put(b.getBytes());
              });
    }
    return length;
  }

  /**
   * read string map from buffer
   *
   * @param buffer ByteBuffer
   * @return Map<String, String>
   */
  public static Map<String, List<String>> readStringMapLists(ByteBuffer buffer) {
    int length = readInt(buffer);
    if (length == -1) {
      return null;
    }
    Map<String, List<String>> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      String key = readString(buffer);
      // value
      int listSize = readInt(buffer);
      List<String> valueList = new ArrayList<>();
      for (int j = 0; j < listSize; j++) {
        String value = readString(buffer);
        valueList.add(value);
      }
      map.put(key, valueList);
    }
    return map;
  }

  /**
   * write a string map to buffer
   *
   * @param map Map<String, String>
   * @param buffer
   * @return length
   */
  public static int writeIntMapLists(Map<Integer, List<Integer>> map, ByteBuffer buffer) {
    if (map == null) {
      return write(-1, buffer);
    }

    int length = 0;
    byte[] bytes;
    buffer.putInt(map.size());
    for (Map.Entry<Integer, List<Integer>> entry : map.entrySet()) {
      buffer.putInt(entry.getKey());
      buffer.putInt(entry.getValue().size());
      entry
          .getValue()
          .forEach(
              b -> {
                buffer.putInt(b);
              });
    }
    return length;
  }

  /**
   * read string map from buffer
   *
   * @param buffer ByteBuffer
   * @return Map<String, String>
   */
  public static Map<Integer, List<Integer>> readIntMapLists(ByteBuffer buffer) {
    int length = readInt(buffer);
    if (length == -1) {
      return null;
    }
    Map<Integer, List<Integer>> map = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      // key
      int key = readInt(buffer);
      // value
      int listSize = readInt(buffer);
      List<Integer> valueList = new ArrayList<>();
      for (int j = 0; j < listSize; j++) {
        int value = readInt(buffer);
        valueList.add(value);
      }
      map.put(key, valueList);
    }
    return map;
  }
}
