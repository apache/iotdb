/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;

public class CommonUtils {

  public static int javaVersion = CommonUtils.getJdkVersion();

  /**
   * get JDK version.
   *
   * @return JDK version (int type)
   */
  public static int getJdkVersion() {
    String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    if (Integer.parseInt(javaVersionElements[0]) == 1) {
      return Integer.parseInt(javaVersionElements[1]);
    } else {
      return Integer.parseInt(javaVersionElements[0]);
    }
  }

  /**
   * clean buffer.
   *
   * @param byteBuffer Buffer
   * @throws Exception Exception
   */
  public static void destroyBuffer(Buffer byteBuffer) throws Exception {
    if (javaVersion == 8) {
      ((DirectBuffer) byteBuffer).cleaner().clean();
    } else {
      try {
        final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafeField.setAccessible(true);
        final Object theUnsafe = theUnsafeField.get(null);
        final Method invokeCleanerMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        invokeCleanerMethod.invoke(theUnsafe, byteBuffer);
      } catch (Exception e) {
        throw e;
      }
    }
  }
}
