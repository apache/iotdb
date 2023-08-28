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
package org.apache.iotdb.commons.concurrent;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import java.util.concurrent.Callable;

/** A wrapper for {@link Callable} logging errors when uncaught exception is thrown. */
public abstract class WrappedCallable<V> implements Callable<V> {

  @Override
  public final V call() {
    try {
      return callMayThrow();
    } catch (Throwable e) {
      throw ScheduledExecutorUtil.propagate(e);
    }
  }

  public abstract V callMayThrow() throws Throwable;

  public static <V> Callable<V> wrap(Callable<V> callable) {
    if (callable instanceof WrappedCallable) {
      return callable;
    }
    return new WrappedCallable<V>() {
      @Override
      public V callMayThrow() throws Exception {
        return callable.call();
      }
    };
  }
}
