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

import org.apache.iotdb.commons.client.ThriftClient;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.thrift.TException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

public class ByteBuddyEnhancer {
  public static <V> V createProxy(Class<V> targetClass, Constructor constructor, Object[] args)
      throws NoSuchMethodException,
          IllegalAccessException,
          InvocationTargetException,
          InstantiationException {
    ElementMatcher.Junction<MethodDescription> matcher =
        ElementMatchers.noneOf(Object.class.getDeclaredMethods());

    return new ByteBuddy()
        .subclass(targetClass)
        .method(matcher)
        .intercept(MethodDelegation.to(SyncThriftClientWithErrorHandler.class))
        .make()
        .load(targetClass.getClassLoader(), ClassLoadingStrategy.Default.CHILD_FIRST)
        .getLoaded()
        .getDeclaredConstructor(constructor.getParameterTypes())
        .newInstance(args);
  }

  public static class SyncThriftClientWithErrorHandler {
    @RuntimeType
    public static Object intercept(
        @This Object targetObject, @SuperCall Callable<?> callable, @Origin Method method)
        throws TException {
      try {
        Object result = callable.call();
        return result;
      } catch (Throwable t) {
        ThriftClient.resolveException(t, (ThriftClient) targetObject);
        throw new TException(
            "Error in calling method " + method.getName() + ", because: " + t.getMessage(), t);
      }
    }
  }
}
