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
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.utils.Pair;
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
   * the key of queryStartTimeMap is the query id and the value of queryStartTimeMap is the start
   * time and the sql of this query.
   */
  private Map<Long, Pair<Long, String>> queryInfoMap;
  /**
   * the key of queryThreadMap is the query id and the value of queryThreadMap is the executing
   * thread of this query.
   * Only main thread is put in this map since the sub threads are maintained by the thread pool.
   * The thread allocated for readTask will change every time, so we have to access this map
   * frequently, which will lead to big performance cost.
   */
  private Map<Long, Thread> queryThreadMap;

  private ScheduledExecutorService executorService;

  private QueryTimeManager() {
    queryInfoMap = new ConcurrentHashMap<>();
    queryThreadMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "query-time-manager");
  }

  public void registerQuery(long queryId, long startTime, String sql, long timeout,
      Thread queryThread) {
    queryInfoMap.put(queryId, new Pair<>(startTime, sql));
    queryThreadMap.put(queryId, queryThread);
    // submit a scheduled task to judge whether query is still running after timeout
    executorService.schedule(() -> {
      queryThreadMap.computeIfPresent(queryId, (k, v) -> {
        killQuery(k);
        logger.error(String.format("Query is time out with queryId %d", queryId));
        return null;
      });
    }, timeout, TimeUnit.MILLISECONDS);
  }

  public void killQuery(long queryId) {
    if (queryThreadMap.get(queryId) == null) {
      return;
    }
    queryThreadMap.get(queryId).interrupt();
    unRegisterQuery(queryId);
  }

  public void unRegisterQuery(long queryId) {
    if (Thread.interrupted()) {
      throw new QueryTimeoutRuntimeException(
          QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
    }
    queryInfoMap.remove(queryId);
    queryThreadMap.remove(queryId);
  }

  public boolean isQueryInterrupted(long queryId) {
    return queryThreadMap.get(queryId).isInterrupted();
  }

  public Map<Long, Pair<Long, String>> getQueryInfoMap() {
    return queryInfoMap;
  }

  public Map<Long, Thread> getQueryThreadMap() {
    return queryThreadMap;
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
}
