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

package org.apache.iotdb.db.query.pool;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.AbstractPoolManager;
import org.apache.iotdb.db.service.metrics.MetricService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * This thread pool is used to read data for raw data query. Thread named by Sub_Raw_Query.
 *
 * <p>Execute ReadTask() in RawQueryReadTaskPoolManager
 */
public class RawQueryReadTaskPoolManager extends AbstractPoolManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RawQueryReadTaskPoolManager.class);

  private RawQueryReadTaskPoolManager() {
    int threadCnt =
        Math.min(
            Runtime.getRuntime().availableProcessors(),
            IoTDBDescriptor.getInstance().getConfig().getConcurrentSubRawQueryThread());
    pool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            threadCnt, ThreadName.SUB_RAW_QUERY_SERVICE.getName());
    MetricService.getInstance().addMetricSet(new RawQueryReadTaskPoolManagerMetrics(this));
  }

  public long getActiveCount() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  public long getWaitingCount() {
    return ((ThreadPoolExecutor) pool).getQueue().size();
  }

  public static RawQueryReadTaskPoolManager getInstance() {
    return RawQueryReadTaskPoolManager.InstanceHolder.instance;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getName() {
    return "raw query read task";
  }

  @Override
  public void start() {
    if (pool == null) {
      int threadCnt =
          Math.min(
              Runtime.getRuntime().availableProcessors(),
              IoTDBDescriptor.getInstance().getConfig().getConcurrentSubRawQueryThread());
      pool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              threadCnt, ThreadName.SUB_RAW_QUERY_SERVICE.getName());
    }
  }

  @Override
  public void stop() {
    if (pool != null) {
      close();
      pool = null;
    }
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      // allowed to do nothing
    }

    private static RawQueryReadTaskPoolManager instance = new RawQueryReadTaskPoolManager();
  }
}
