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
package org.apache.iotdb.commons.client.sync;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;

public class SyncThriftClientWithErrorHandler implements MethodInterceptor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SyncThriftClientWithErrorHandler.class);

  public static <V extends SyncThriftClient> V newErrorHandler(
      Class<V> targetClass, Constructor<V> constructor, Object... args) {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(targetClass);
    enhancer.setCallback(new SyncThriftClientWithErrorHandler());
    if (constructor == null) {
      return (V) enhancer.create();
    }
    return (V) enhancer.create(constructor.getParameterTypes(), args);
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    try {
      return methodProxy.invokeSuper(o, objects);
    } catch (Throwable t) {
      Throwable origin = t;
      if (t instanceof InvocationTargetException) {
        origin = ((InvocationTargetException) t).getTargetException();
      }
      Throwable cur = origin;
      if (cur instanceof TException) {
        int level = 0;
        while (cur != null) {
          LOGGER.error(
              "level-{} Exception class {}, message {}",
              level,
              cur.getClass().getName(),
              cur.getMessage());
          cur = cur.getCause();
          level++;
        }
        ((SyncThriftClient) o).invalidate();
      }

      Throwable rootCause = ExceptionUtils.getRootCause(origin);
      if (rootCause != null) {
        // if the exception is SocketException and its error message is Broken pipe, it means that
        // the remote node may restart and all the connection we cached before should be cleared.
        LOGGER.error(
            "root cause message {}, LocalizedMessage {}, ",
            rootCause.getMessage(),
            rootCause.getLocalizedMessage(),
            rootCause);
        if (isConnectionBroken(rootCause)) {
          LOGGER.error(
              "Broken pipe error happened in calling method {}, we need to clear all previous cached connection, err: {}",
              method.getName(),
              t);
          ((SyncThriftClient) o).invalidate();
          ((SyncThriftClient) o).invalidateAll();
        }
      }
      throw new TException("Error in calling method " + method.getName(), t);
    }
  }

  private boolean isConnectionBroken(Throwable cause) {
    return (cause instanceof SocketException && cause.getMessage().contains("Broken pipe"))
        || (cause instanceof TTransportException
            && cause.getMessage().contains("Socket is closed by peer"));
  }
}
