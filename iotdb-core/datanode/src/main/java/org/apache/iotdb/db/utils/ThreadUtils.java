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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** the utils for managing thread or thread pool */
public class ThreadUtils {

  private static final Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

  public static void stopThreadPool(ExecutorService pool, ThreadName poolName) {
    if (pool != null) {
      pool.shutdownNow();
      try {
        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
          logger.warn("Waiting {} to be terminated is timeout", poolName.getName());
        }
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 60s", poolName.getName());
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(
            String.format("StorageEngine failed to stop because of %s.", poolName.getName()), e);
      }
    }
  }
}
