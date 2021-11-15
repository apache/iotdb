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

package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to monitor the executing time of each query. Once one is over the threshold,
 * it will be killed and return the time out exception.
 */
public class QueryTimeManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(QueryTimeManager.class);

  private Map<Long, QueryContext> queryContextMap;

  private ScheduledExecutorService executorService;

  private Map<Long, ScheduledFuture<?>> queryScheduledTaskMap;

  private QueryTimeManager() {
    queryContextMap = new ConcurrentHashMap<>();
    queryScheduledTaskMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1, "query-time-manager");
  }

  public void registerQuery(QueryContext context) {
    queryContextMap.put(context.getQueryId(), context);
    // Use the default configuration of server if a negative timeout
    if (context.getTimeout() < 0) {
      context.setTimeout(IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    }
    if (context.getTimeout() != 0) {
      // submit a scheduled task to judge whether query is still running after timeout
      ScheduledFuture<?> scheduledFuture =
          executorService.schedule(
              () -> {
                killQuery(context.getQueryId());
                logger.warn(
                    String.format(
                        "Query is time out (%dms) with queryId %d",
                        context.getTimeout(), context.getQueryId()));
              },
              context.getTimeout(),
              TimeUnit.MILLISECONDS);
      queryScheduledTaskMap.put(context.getQueryId(), scheduledFuture);
    }
  }

  public void killQuery(long queryId) {
    if (queryContextMap.get(queryId) == null) {
      return;
    }
    queryContextMap.get(queryId).setInterrupted(true);
  }

  /**
   * UnRegister query when query quits because of getting enough data or timeout. If getting enough
   * data, we only remove the timeout task. If the query is full quit because of timeout or
   * EndQuery(), we remove them all.
   *
   * @param fullQuit True if timeout or endQuery()
   */
  public AtomicBoolean unRegisterQuery(long queryId, boolean fullQuit) {
    // This is used to make sure the QueryTimeoutRuntimeException is thrown once
    AtomicBoolean successRemoved = new AtomicBoolean(false);
    queryContextMap.computeIfPresent(
        queryId,
        (k, v) -> {
          successRemoved.set(true);
          ScheduledFuture<?> scheduledFuture = queryScheduledTaskMap.remove(queryId);
          if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
          }
          SessionTimeoutManager.getInstance()
              .refresh(SessionManager.getInstance().getSessionIdByQueryId(queryId));
          return fullQuit ? null : v;
        });
    return successRemoved;
  }

  /**
   * Check given query is alive or not. We only throw the queryTimeoutRunTimeException once. If the
   * runTimeException is thrown in main thread, it will quit directly while the return value will be
   * used to ask sub query threads to quit. Else if it's thrown in one sub thread, other sub threads
   * will quit by reading the return value, and main thread will catch and throw the same exception
   * by reading the ExceptionBatchData.
   *
   * @return True if alive.
   */
  public static boolean checkQueryAlive(long queryId) {
    QueryContext queryContext = getInstance().getQueryContext(queryId);
    if (queryContext == null) {
      return false;
    } else if (queryContext.isInterrupted()) {
      if (getInstance().unRegisterQuery(queryId, true).get()) {
        throw new QueryTimeoutRuntimeException();
      }
      return false;
    }
    return true;
  }

  public Map<Long, QueryContext> getQueryContextMap() {
    return queryContextMap;
  }

  public void clear() {
    queryContextMap.clear();
    queryScheduledTaskMap.clear();
  }

  public QueryContext getQueryContext(long queryId) {
    return queryContextMap.get(queryId);
  }

  public static QueryTimeManager getInstance() {
    return QueryTimeManagerHelper.INSTANCE;
  }

  @Override
  public void start() {
    // Do Nothing
  }

  @Override
  public void stop() {
    if (executorService == null || executorService.isShutdown()) {
      return;
    }
    executorService.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.QUERY_TIME_MANAGER;
  }

  private static class QueryTimeManagerHelper {

    private static final QueryTimeManager INSTANCE = new QueryTimeManager();

    private QueryTimeManagerHelper() {}
  }
}
