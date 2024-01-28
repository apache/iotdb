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
package org.apache.iotdb.db.experiment;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class InsertWorkers {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertWorkers.class);
  private static ThreadPoolExecutor threadPool = null;

  static {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableMultiThreadingInsert()) {
      LOGGER.info("Initializing thread pool for parallel insert");
      threadPool =
          new ThreadPoolExecutor(
              Runtime.getRuntime().availableProcessors(),
              IoTDBDescriptor.getInstance().getConfig().getInsertThreadNum(),
              10,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(8196),
              new ThreadPoolExecutor.CallerRunsPolicy());
    }
  }

  public static void submit(InsertTask task) {
    threadPool.submit(task);
  }
}
