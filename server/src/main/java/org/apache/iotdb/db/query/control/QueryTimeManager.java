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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to monitor the executing time of each query.
 * </p>
 * Once one is over the threshold, it will be killed and return the time out exception.
 */
public class QueryTimeManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(QueryTimeManager.class);

  /**
   * the key of queryInfoMap is the query id and the value of queryInfoMap is the start
   * time, the statement and executing thread of this query.
   */
  private Map<Long, QueryInfo> queryInfoMap;

  private ScheduledExecutorService executorService;

  private Map<Long, ScheduledFuture<?>> queryScheduledTaskMap;

  private QueryTimeManager() {
    queryInfoMap = new ConcurrentHashMap<>();
    queryScheduledTaskMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "query-time-manager");
  }

  public void registerQuery(long queryId, long startTime, String sql, long timeout,
      Thread queryThread) {
    queryInfoMap.put(queryId, new QueryInfo(startTime, sql, queryThread));
    // submit a scheduled task to judge whether query is still running after timeout
    ScheduledFuture<?> scheduledFuture = executorService.schedule(() -> {
      queryInfoMap.computeIfPresent(queryId, (k, v) -> {
        killQuery(k);
        logger.error(String.format("Query is time out with queryId %d", queryId));
        return null;
      });
    }, timeout, TimeUnit.MILLISECONDS);
    queryScheduledTaskMap.put(queryId, scheduledFuture);
  }

  public void killQuery(long queryId) {
    if (queryInfoMap.get(queryId) == null) {
      return;
    }
    queryInfoMap.get(queryId).getThread().interrupt();
    unRegisterQuery(queryId);
  }

  public void unRegisterQuery(long queryId) {
    if (Thread.interrupted()) {
      throw new QueryTimeoutRuntimeException(
          QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
    }
    if (queryInfoMap.get(queryId) == null) {
      return;
    }
    queryInfoMap.remove(queryId);
    queryScheduledTaskMap.get(queryId).cancel(false);
    queryScheduledTaskMap.remove(queryId);
  }

  public boolean isQueryInterrupted(long queryId) {
    return queryInfoMap.get(queryId).getThread().isInterrupted();
  }

  public Map<Long, QueryInfo> getQueryInfoMap() {
    return queryInfoMap;
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

    private QueryTimeManagerHelper() {
    }
  }

  public class QueryInfo {

    /**
     *  To reduce the cost of memory, we only keep the a certain size statement.
     *  For statement whose length is over this, we keep its head and tail.
     */
    private static final int MAX_STATEMENT_LENGTH = 64;

    private final long startTime;
    private final String statement;
    /**
     *  Only main thread is put in this structure since the sub threads are maintained
     *  by the thread pool. The thread allocated for readTask will change every time,
     *  so we have to access this map frequently, which will lead to big performance cost.
     */
    private final Thread thread;

    public QueryInfo(long startTime, String statement, Thread thread) {
      this.startTime = startTime;
      this.thread = thread;
      if (statement.length() <= 64) {
        this.statement = statement;
      } else {
        this.statement = statement.substring(0, MAX_STATEMENT_LENGTH / 2) + "..." + statement
            .substring(statement.length() - MAX_STATEMENT_LENGTH / 2);
      }
    }

    public long getStartTime() {
      return startTime;
    }

    public String getStatement() {
      return statement;
    }

    public Thread getThread() {
      return thread;
    }
  }
}
