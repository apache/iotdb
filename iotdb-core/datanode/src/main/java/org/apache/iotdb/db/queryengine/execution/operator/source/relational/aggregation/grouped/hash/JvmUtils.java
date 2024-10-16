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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import com.google.common.base.Preconditions;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteOrder;

public class JvmUtils {
  static final Unsafe unsafe;
  private static final long ADDRESS_OFFSET;

  private static void assertArrayIndexScale(
      String name, int actualIndexScale, int expectedIndexScale) {
    if (actualIndexScale != expectedIndexScale) {
      throw new IllegalStateException(
          name
              + " array index scale must be "
              + expectedIndexScale
              + ", but is "
              + actualIndexScale);
    }
  }

  static long bufferAddress(Buffer buffer) {
    Preconditions.checkArgument(buffer.isDirect(), "buffer is not direct");
    return unsafe.getLong(buffer, ADDRESS_OFFSET);
  }

  private JvmUtils() {}

  static {
    if (!ByteOrder.LITTLE_ENDIAN.equals(ByteOrder.nativeOrder())) {
      throw new UnsupportedOperationException("Slice only supports little endian machines.");
    } else {
      try {
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        unsafe = (Unsafe) field.get((Object) null);
        if (unsafe == null) {
          throw new RuntimeException("Unsafe access not available");
        } else {
          assertArrayIndexScale("Boolean", Unsafe.ARRAY_BOOLEAN_INDEX_SCALE, 1);
          assertArrayIndexScale("Byte", Unsafe.ARRAY_BYTE_INDEX_SCALE, 1);
          assertArrayIndexScale("Short", Unsafe.ARRAY_SHORT_INDEX_SCALE, 2);
          assertArrayIndexScale("Int", Unsafe.ARRAY_INT_INDEX_SCALE, 4);
          assertArrayIndexScale("Long", Unsafe.ARRAY_LONG_INDEX_SCALE, 8);
          assertArrayIndexScale("Float", Unsafe.ARRAY_FLOAT_INDEX_SCALE, 4);
          assertArrayIndexScale("Double", Unsafe.ARRAY_DOUBLE_INDEX_SCALE, 8);
          ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        }
      } catch (ReflectiveOperationException var1) {
        throw new RuntimeException(var1);
      }
    }
  }
}
