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
package org.apache.iotdb.itbase.runtime;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A singleton thread pool used for handling test requests in one single JVM. This can be used to
 * avoid thread leak and frequency thread allocation.
 */
public class RequestThreadPool {
  private final ExecutorService executor;

  private RequestThreadPool() {
    executor = Executors.newFixedThreadPool(5);
  }

  public static <T> Future<T> submit(Callable<T> task) {
    return InstanceHolder.INSTANCE.executor.submit(task);
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      // nothing to do
    }

    private static final RequestThreadPool INSTANCE = new RequestThreadPool();
  }
}
