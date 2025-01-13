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

package org.apache.iotdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

public class MagicUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(MagicUtils.class);

  /**
   * Ignore all exceptions during close()
   *
   * @param t target object
   * @return object which will close without exception
   */
  public static <T extends AutoCloseable> T makeItCloseQuietly(T t) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          try {
            if (method.getName().equals("close")) {
              try {
                method.invoke(t, args);
              } catch (Throwable e) {
                LOGGER.warn("Exception happens during close(): ", e);
              }
              return null;
            } else {
              return method.invoke(t, args);
            }
          } catch (InvocationTargetException e) {
            throw e.getTargetException();
          }
        };
    return (T)
        Proxy.newProxyInstance(
            t.getClass().getClassLoader(), t.getClass().getInterfaces(), handler);
  }
}
