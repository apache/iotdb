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

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class SyncThriftClientErrorHandler<T extends SyncThriftClient> implements InvocationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncThriftClientErrorHandler.class);

  private final T client;

  private SyncThriftClientErrorHandler(T client) {
    this.client = client;
  }

  public static <V extends SyncThriftClient> V newErrorHandlerClient(
      V target, Class<? super V> clazz) {
    return (V)
        Proxy.newProxyInstance(
            target.getClass().getClassLoader(),
            new Class[] {clazz},
            new SyncThriftClientErrorHandler<V>(target));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(client, args);
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof TException) {
        LOGGER.error(
            "Error in calling method {}, err: {}", method.getName(), e.getTargetException());
        client.invalidate();
      }
      throw new TException("Error in calling method " + method.getName(), e.getTargetException());
    } catch (Exception e) {
      throw new TException("Error in calling method " + method.getName(), e);
    }
  }
}
