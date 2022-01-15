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

package org.apache.iotdb.session.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadUtils {

  /**
   * @param threadNamePrefix thread name prefix, the thread name will be "prefix-num"
   * @param isDaemon if true, the thread be created will be daemon
   * @return
   */
  public static ThreadFactory createThreadFactory(String threadNamePrefix, boolean isDaemon) {
    return new ThreadFactory() {
      private final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(String.format("%s-%d", threadNamePrefix, THREAD_COUNT.getAndIncrement()));
        thread.setDaemon(isDaemon);
        return thread;
      }
    };
  }
}
