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

package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ContinuousQueryException;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ContinuousQueryService implements IService {

  private static long CHECK_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval() / 2;

  private static final Logger logger = LoggerFactory.getLogger(ContinuousQueryService.class);

  private final ConcurrentHashMap<String, CreateContinuousQueryPlan> continuousQueryPlans =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, Long> nextExecutionTimestamps = new ConcurrentHashMap<>();

  private final ReentrantLock registrationLock = new ReentrantLock();

  private static final ContinuousQueryService INSTANCE = new ContinuousQueryService();

  private ScheduledExecutorService checkThread;

  protected static final ContinuousQueryTaskPoolManager TASK_POOL_MANAGER =
      ContinuousQueryTaskPoolManager.getInstance();

  private ContinuousQueryService() {}

  public static ContinuousQueryService getInstance() {
    return INSTANCE;
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONTINUOUS_QUERY_SERVICE;
  }

  @Override
  public void start() {

    for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {
      long durationFromCreation = DatetimeUtils.currentTime() - plan.getCreationTimestamp();
      long nextExecutionTimestamp =
          plan.getCreationTimestamp()
              + plan.getEveryInterval()
                  * (durationFromCreation / plan.getEveryInterval()
                      + ((durationFromCreation % plan.getEveryInterval() == 0) ? 0 : 1));
      nextExecutionTimestamps.put(plan.getContinuousQueryName(), nextExecutionTimestamp);
    }

    checkThread = Executors.newSingleThreadScheduledExecutor();
    checkThread.scheduleAtFixedRate(
        this::checkAndSubmitTasks,
        0,
        CHECK_INTERVAL,
        DatetimeUtils.timestampPrecisionStringToTimeUnit(
            IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision()));

    logger.info("Continuous query service started.");
  }

  @Override
  public void stop() {
    if (checkThread != null) {
      checkThread.shutdown();
      try {
        checkThread.awaitTermination(600, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.warn("Check thread still doesn't exit after 60s");
        checkThread.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    stop();
  }

  private void checkAndSubmitTasks() {
    long currentTimestamp = DatetimeUtils.currentTime();

    for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {
      long nextExecutionTimestamp = nextExecutionTimestamps.get(plan.getContinuousQueryName());
      while (currentTimestamp >= nextExecutionTimestamp) {
        TASK_POOL_MANAGER.submit(new ContinuousQueryTask(plan, nextExecutionTimestamp));
        nextExecutionTimestamp += plan.getEveryInterval();
      }
      nextExecutionTimestamps.replace(plan.getContinuousQueryName(), nextExecutionTimestamp);
    }
  }

  public boolean register(CreateContinuousQueryPlan plan, boolean writeLog)
      throws ContinuousQueryException {

    acquireRegistrationLock();

    try {
      if (continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
        throw new ContinuousQueryException(
            String.format("Continuous Query [%s] already exists", plan.getContinuousQueryName()));
      }
      if (writeLog) {
        IoTDB.metaManager.createContinuousQuery(plan);
      }
      doRegister(plan);
    } catch (ContinuousQueryException e) {
      throw e;
    } catch (Exception e) {
      throw new ContinuousQueryException(e.getMessage());
    } finally {
      releaseRegistrationLock();
    }
    return true;
  }

  private void doRegister(CreateContinuousQueryPlan plan) {
    continuousQueryPlans.put(plan.getContinuousQueryName(), plan);
    nextExecutionTimestamps.put(plan.getContinuousQueryName(), plan.getCreationTimestamp());
  }

  public void deregisterAll() throws ContinuousQueryException {
    for (String cqName : continuousQueryPlans.keySet()) {
      deregister(new DropContinuousQueryPlan(cqName));
    }
  }

  public boolean deregister(DropContinuousQueryPlan plan) throws ContinuousQueryException {

    acquireRegistrationLock();

    try {
      if (!continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
        throw new ContinuousQueryException(
            String.format("Continuous Query [%s] does not exist", plan.getContinuousQueryName()));
      }
      IoTDB.metaManager.dropContinuousQuery(plan);
      doDeregister(plan);
    } catch (ContinuousQueryException e) {
      throw e;
    } catch (Exception e) {
      throw new ContinuousQueryException(e.getMessage());
    } finally {
      releaseRegistrationLock();
    }

    return true;
  }

  private void doDeregister(DropContinuousQueryPlan plan) {
    continuousQueryPlans.remove(plan.getContinuousQueryName());
    nextExecutionTimestamps.remove(plan.getContinuousQueryName());
  }

  public List<ShowContinuousQueriesResult> getShowContinuousQueriesResultList() {

    List<ShowContinuousQueriesResult> results = new ArrayList<>(continuousQueryPlans.size());

    for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {

      results.add(
          new ShowContinuousQueriesResult(
              plan.getQuerySql(),
              plan.getContinuousQueryName(),
              plan.getTargetPath(),
              plan.getEveryInterval(),
              plan.getForInterval()));
    }

    return results;
  }
}
