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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.UtilMessages;

import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Utilities for clearing controllable in-memory copies of sensitive data before reuse. */
public final class ResidualDataProtectionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResidualDataProtectionUtils.class);

  private ResidualDataProtectionUtils() {
    // Utility class.
  }

  public static void eraseArrayIfEnabled(Object array, String source) {
    if (!CommonDescriptor.getInstance().getConfig().isEnableSecureErase() || array == null) {
      return;
    }

    final int length;
    final String type;
    if (array instanceof byte[]) {
      final byte[] values = (byte[]) array;
      Arrays.fill(values, (byte) 0);
      length = values.length;
      type = byte.class.getSimpleName();
    } else if (array instanceof boolean[]) {
      final boolean[] values = (boolean[]) array;
      Arrays.fill(values, false);
      length = values.length;
      type = boolean.class.getSimpleName();
    } else if (array instanceof int[]) {
      final int[] values = (int[]) array;
      Arrays.fill(values, 0);
      length = values.length;
      type = int.class.getSimpleName();
    } else if (array instanceof long[]) {
      final long[] values = (long[]) array;
      Arrays.fill(values, 0L);
      length = values.length;
      type = long.class.getSimpleName();
    } else if (array instanceof float[]) {
      final float[] values = (float[]) array;
      Arrays.fill(values, 0.0F);
      length = values.length;
      type = float.class.getSimpleName();
    } else if (array instanceof double[]) {
      final double[] values = (double[]) array;
      Arrays.fill(values, 0.0D);
      length = values.length;
      type = double.class.getSimpleName();
    } else if (array instanceof Binary[]) {
      final Binary[] values = (Binary[]) array;
      for (final Binary value : values) {
        if (value != null) {
          final byte[] binaryValues = value.getValues();
          if (binaryValues != null) {
            Arrays.fill(binaryValues, (byte) 0);
          }
        }
      }
      length = values.length;
      type = Binary.class.getSimpleName();
    } else {
      return;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          UtilMessages.LOG_SECURELY_ERASED_ARRAY_FOR_ARG_TYPE_ARG_LENGTH_ARG_11E18B5E,
          source,
          type,
          length);
    }
  }

  public static void eraseByteBufferIfEnabled(ByteBuffer buffer, String source) {
    if (!CommonDescriptor.getInstance().getConfig().isEnableSecureErase()) {
      return;
    }
    eraseByteBuffer(buffer, source);
  }

  public static void eraseByteBuffer(ByteBuffer buffer, String source) {
    if (buffer == null) {
      return;
    }

    final ByteBuffer duplicate = buffer.duplicate();
    duplicate.clear();
    while (duplicate.hasRemaining()) {
      duplicate.put((byte) 0);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          UtilMessages.LOG_SECURELY_ERASED_MEMORY_BUFFER_FOR_ARG_CAPACITY_ARG_BYTES_2F7CAB4C,
          source,
          buffer.capacity());
    }
  }

  public static void erasePassword(char[] password, String source) {
    if (password == null) {
      return;
    }
    Arrays.fill(password, '\0');
    logPasswordErase(source, password.length);
  }

  public static void erasePassword(byte[] password, String source) {
    if (password == null) {
      return;
    }
    Arrays.fill(password, (byte) 0);
    logPasswordErase(source, password.length);
  }

  private static void logPasswordErase(String source, int length) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          UtilMessages.LOG_SECURELY_ERASED_PASSWORD_BUFFER_FOR_ARG_LENGTH_ARG_F407256A,
          source,
          length);
    }
  }
}
