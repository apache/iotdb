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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Utility class for parsing Object type data (BLOB). Object data storage format: - First 8 bytes
 * (Long): file offset + content length - Following bytes: file relative path (UTF-8 encoded)
 */
public class ObjectDataParser {

  /** Long type byte length */
  private static final int LONG_BYTES = Long.BYTES;

  /**
   * Parse Binary object to extract Object data information
   *
   * @param binary Binary object
   * @return Pair(offsetPlusLength, relativePath), null if parse failed
   */
  public static Pair<Long, String> parse(Binary binary) {
    if (Objects.isNull(binary)) {
      return null;
    }

    byte[] bytes = binary.getValues();
    return parse(bytes);
  }

  /**
   * Parse byte array to extract Object data information
   *
   * @param bytes byte array
   * @return Pair(offsetPlusLength, relativePath), null if parse failed
   */
  public static Pair<Long, String> parse(byte[] bytes) {
    if (Objects.isNull(bytes) || bytes.length < LONG_BYTES) {
      return null;
    }

    try {
      // Parse first 8 bytes: length
      long offsetPlusLength = BytesUtils.bytesToLong(bytes, 0);

      // Parse following bytes: file path
      int pathLength = bytes.length - LONG_BYTES;
      if (pathLength <= 0) {
        return null;
      }

      byte[] pathBytes = new byte[pathLength];
      System.arraycopy(bytes, LONG_BYTES, pathBytes, 0, pathLength);
      String relativePath = new String(pathBytes, StandardCharsets.UTF_8);

      return new Pair<>(offsetPlusLength, relativePath);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract file length information from byte array
   *
   * @param bytes byte array
   * @return offset + content.length, -1 if extraction failed
   */
  public static long extractLength(byte[] bytes) {
    if (Objects.isNull(bytes) || bytes.length < LONG_BYTES) {
      return -1;
    }

    try {
      return BytesUtils.bytesToLong(bytes, 0);
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Extract file length information from Binary object
   *
   * @param binary Binary object
   * @return offset + content.length, -1 if extraction failed
   */
  public static long extractLength(Binary binary) {
    if (Objects.isNull(binary)) {
      return -1;
    }
    return extractLength(binary.getValues());
  }

  /**
   * Extract file relative path from byte array
   *
   * @param bytes byte array
   * @return file relative path, null if extraction failed
   */
  public static String extractPath(byte[] bytes) {
    if (Objects.isNull(bytes) || bytes.length <= LONG_BYTES) {
      return null;
    }

    try {
      int pathLength = bytes.length - LONG_BYTES;
      byte[] pathBytes = new byte[pathLength];
      System.arraycopy(bytes, LONG_BYTES, pathBytes, 0, pathLength);
      return new String(pathBytes, StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract file relative path from Binary object
   *
   * @param binary Binary object
   * @return file relative path, null if extraction failed
   */
  public static String extractPath(Binary binary) {
    if (Objects.isNull(binary)) {
      return null;
    }
    return extractPath(binary.getValues());
  }

  /**
   * Validate if byte array is valid Object data format
   *
   * @param bytes byte array
   * @return true if format is valid, false otherwise
   */
  public static boolean isValidObjectData(byte[] bytes) {
    if (Objects.isNull(bytes) || bytes.length <= LONG_BYTES) {
      return false;
    }

    try {
      // Try to parse length
      long length = BytesUtils.bytesToLong(bytes, 0);
      if (length < 0) {
        return false;
      }

      // Try to parse path
      int pathLength = bytes.length - LONG_BYTES;
      byte[] pathBytes = new byte[pathLength];
      System.arraycopy(bytes, LONG_BYTES, pathBytes, 0, pathLength);
      String path = new String(pathBytes, StandardCharsets.UTF_8);

      return path != null && !path.isEmpty();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Validate if Binary object is valid Object data format
   *
   * @param binary Binary object
   * @return true if format is valid, false otherwise
   */
  public static boolean isValidObjectData(Binary binary) {
    if (Objects.isNull(binary)) {
      return false;
    }
    return isValidObjectData(binary.getValues());
  }
}
