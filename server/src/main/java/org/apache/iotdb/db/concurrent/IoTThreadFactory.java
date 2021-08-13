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
package org.apache.iotdb.db.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTThreadFactory implements ThreadFactory {

  private static final AtomicInteger poolNumber = new AtomicInteger(1);
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;
  private Thread.UncaughtExceptionHandler handler = new IoTDBDefaultThreadExceptionHandler();

  /** Constructor of IoTThreadFactory. */
  public IoTThreadFactory(String poolName) {
    SecurityManager s = System.getSecurityManager();
    group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    // thread pool name format : pool-number-IoTDB-poolName-thread-
    this.namePrefix = "pool-" + poolNumber.getAndIncrement() + "-IoTDB-" + poolName + "-";
  }

  public IoTThreadFactory(String poolName, Thread.UncaughtExceptionHandler handler) {
    this(poolName);
    this.handler = handler;
  }

  @Override
  public Thread newThread(Runnable r) {
    // thread name format : pool-number-IoTDB-poolName-thread-threadnum
    Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
    if (t.isDaemon()) {
      t.setDaemon(false);
    }
    if (t.getPriority() != Thread.NORM_PRIORITY) {
      t.setPriority(Thread.NORM_PRIORITY);
    }
    t.setUncaughtExceptionHandler(handler);
    return t;
  }
}
