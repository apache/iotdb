/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.utils;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class MmapUtil {

  public static void clean(MappedByteBuffer mappedByteBuffer) {
    if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() || mappedByteBuffer.capacity()== 0)
      return;
    invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
  }

  private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
    return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
      try {
        Method method = method(target, methodName, args);
        method.setAccessible(true);
        return method.invoke(target);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private static Method method(Object target, String methodName, Class<?>[] args)
      throws NoSuchMethodException {
    try {
      return target.getClass().getMethod(methodName, args);
    } catch (NoSuchMethodException e) {
      return target.getClass().getDeclaredMethod(methodName, args);
    }
  }

  private static ByteBuffer viewed(ByteBuffer buffer) {
    String methodName = "viewedBuffer";
    Method[] methods = buffer.getClass().getMethods();
    for (Method method : methods) {
      if (method.getName().equals("attachment")) {
        methodName = "attachment";
        break;
      }
    }
    ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
    if (viewedBuffer == null)
      return buffer;
    else
      return viewed(viewedBuffer);
  }
}
