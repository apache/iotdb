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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pool is used to execute all query task send from client, and return TSExecuteStatementResp.
 * Thread named by Query.
 *
 * <p>Execute QueryTask() in TSServiceImpl
 */
public class QueryTaskManager extends AbstractPoolManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryTaskManager.class);

  private QueryTaskManager() {
    int threadCnt =
        Math.min(
            Runtime.getRuntime().availableProcessors(),
            IoTDBDescriptor.getInstance().getConfig().getQueryThreadCount());
    pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.QUERY_SERVICE.getName());
  }

  public static QueryTaskManager getInstance() {
    return QueryTaskManager.InstanceHolder.instance;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getName() {
    return "query task";
  }

  @Override
  public void start() {
    if (pool == null) {
      int threadCnt =
          Math.min(
              Runtime.getRuntime().availableProcessors(),
              IoTDBDescriptor.getInstance().getConfig().getQueryThreadCount());
      pool =
          IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.QUERY_SERVICE.getName());
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

    private static QueryTaskManager instance = new QueryTaskManager();
  }
}
