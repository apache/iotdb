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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof TException) {
        LOGGER.error(
            "Error in calling method {}, err: {}", method.getName(), e.getTargetException());
        ((SyncThriftClient) o).invalidate();
      }
      throw new TException("Error in calling method " + method.getName(), e.getTargetException());
    } catch (Exception e) {
      throw new TException("Error in calling method " + method.getName(), e);
    }
  }
}
