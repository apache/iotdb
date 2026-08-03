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
import org.apache.iotdb.commons.i18n.ClientMessages;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.thrift.TException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

public class SyncThriftClientWithErrorHandler implements MethodInterceptor {

  private static final BiConsumer<Throwable, ThriftClient> NO_OP_FAILURE_HANDLER =
      (failure, client) -> {
        // Do nothing.
      };

  private final BiConsumer<Throwable, ThriftClient> failureHandler;
  private final ThreadLocal<InvocationContext> invocationContext =
      ThreadLocal.withInitial(InvocationContext::new);

  public SyncThriftClientWithErrorHandler() {
    this(NO_OP_FAILURE_HANDLER);
  }

  private SyncThriftClientWithErrorHandler(
      final BiConsumer<Throwable, ThriftClient> failureHandler) {
    this.failureHandler = failureHandler;
  }

  /**
   * Note: The caller needs to ensure that the constructor corresponds to the class, or the cast
   * might fail.
   */
  @SuppressWarnings("unchecked")
  public static <V extends ThriftClient> V newErrorHandler(
      Class<V> targetClass, Constructor<V> constructor, Object... args) {
    return createErrorHandler(targetClass, constructor, NO_OP_FAILURE_HANDLER, args);
  }

  @SuppressWarnings("unchecked")
  public static <V extends ThriftClient> V newErrorHandlerWithFailureHandler(
      Class<V> targetClass,
      Constructor<V> constructor,
      BiConsumer<Throwable, V> failureHandler,
      Object... args) {
    return createErrorHandler(
        targetClass,
        constructor,
        (failure, client) -> failureHandler.accept(failure, (V) client),
        args);
  }

  @SuppressWarnings("unchecked")
  private static <V extends ThriftClient> V createErrorHandler(
      Class<V> targetClass,
      Constructor<V> constructor,
      BiConsumer<Throwable, ThriftClient> failureHandler,
      Object... args) {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(targetClass);
    enhancer.setCallback(new SyncThriftClientWithErrorHandler(failureHandler));
    if (constructor == null) {
      return (V) enhancer.create();
    }
    return (V) enhancer.create(constructor.getParameterTypes(), args);
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    final InvocationContext context = invocationContext.get();
    context.depth++;
    try {
      return methodProxy.invokeSuper(o, objects);
    } catch (Throwable t) {
      if (!context.failureReported) {
        context.failureReported = true;
        try {
          failureHandler.accept(t, (ThriftClient) o);
        } catch (final RuntimeException reportingFailure) {
          if (reportingFailure != t) {
            t.addSuppressed(reportingFailure);
          }
        }
      }
      ThriftClient.resolveException(t, (ThriftClient) o);
      throw new TException(
          ClientMessages.EXCEPTION_ERROR_CALLING_METHOD_C04E5A63
              + method.getName()
              + ClientMessages.EXCEPTION_BECAUSE_ACD0B1C8
              + t.getMessage(),
          t);
    } finally {
      context.depth--;
      if (context.depth == 0) {
        invocationContext.remove();
      }
    }
  }

  private static final class InvocationContext {

    private int depth;
    private boolean failureReported;
  }
}
