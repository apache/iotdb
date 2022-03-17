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

package org.apache.iotdb.db.engine.cq;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.ContinuousQueryException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ContinuousQueryService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousQueryService.class);

  private static final long SYSTEM_STARTUP_TIME = DatetimeUtils.currentTime();

  private static final ContinuousQueryTaskPoolManager TASK_POOL_MANAGER =
      ContinuousQueryTaskPoolManager.getInstance();
  private static final long TASK_SUBMIT_CHECK_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval() / 2;

  private static final String LOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "cq"
          + File.separator;
  private static final String LOG_FILE_NAME = LOG_FILE_DIR + "cqlog.bin";

  private ScheduledExecutorService continuousQueryTaskSubmitThread;

  private final ConcurrentHashMap<String, CreateContinuousQueryPlan> continuousQueryPlans =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> nextExecutionTimestamps = new ConcurrentHashMap<>();

  private CQLogWriter logWriter;

  public void doRecovery() throws StartupException {
    try {
      File logDir = SystemFileFactory.INSTANCE.getFile(LOG_FILE_DIR);
      if (!logDir.exists()) {
        logDir.mkdir();
        return;
      }
      File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);
      if (!logFile.exists()) {
        return;
      }
      try (CQLogReader logReader = new CQLogReader(logFile)) {
        PhysicalPlan plan;
        while (logReader.hasNext()) {
          plan = logReader.next();
          switch (plan.getOperatorType()) {
            case CREATE_CONTINUOUS_QUERY:
              CreateContinuousQueryPlan createContinuousQueryPlan =
                  (CreateContinuousQueryPlan) plan;
              register(createContinuousQueryPlan, false);
              break;
            case DROP_CONTINUOUS_QUERY:
              DropContinuousQueryPlan dropContinuousQueryPlan = (DropContinuousQueryPlan) plan;
              deregister(dropContinuousQueryPlan, false);
              break;
            default:
              LOGGER.error("Unrecognizable command {}", plan.getOperatorType());
          }
        }
      }
    } catch (ContinuousQueryException | IOException e) {
      LOGGER.error("Error occurred during restart CQService");
      throw new StartupException(e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONTINUOUS_QUERY_SERVICE;
  }

  @Override
  public void start() throws StartupException {
    try {
      doRecovery();
      logWriter = new CQLogWriter(LOG_FILE_NAME);

      for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {
        nextExecutionTimestamps.put(
            plan.getContinuousQueryName(),
            calculateNextExecutionTimestamp(plan, SYSTEM_STARTUP_TIME));
      }

      continuousQueryTaskSubmitThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("CQ-Task-Submit-Thread");
      continuousQueryTaskSubmitThread.scheduleAtFixedRate(
          this::checkAndSubmitTasks,
          0,
          TASK_SUBMIT_CHECK_INTERVAL,
          DatetimeUtils.timestampPrecisionStringToTimeUnit(
              IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision()));

      LOGGER.info("Continuous query service started.");
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private long calculateNextExecutionTimestamp(
      CreateContinuousQueryPlan plan, final long currentTime) {
    final long expectedFirstExecutionTime =
        plan.getFirstExecutionTimeBoundary() + plan.getForInterval();

    if (currentTime <= expectedFirstExecutionTime) {
      return expectedFirstExecutionTime;
    }

    final long durationFromExpectedFirstExecutionTime = currentTime - expectedFirstExecutionTime;
    final long everyInterval = plan.getEveryInterval();
    return expectedFirstExecutionTime
        + everyInterval
            * (durationFromExpectedFirstExecutionTime / everyInterval
                + (durationFromExpectedFirstExecutionTime % everyInterval == 0 ? 0 : 1));
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

  @Override
  public void stop() {
    try {
      if (continuousQueryTaskSubmitThread != null) {
        continuousQueryTaskSubmitThread.shutdown();
        try {
          continuousQueryTaskSubmitThread.awaitTermination(600, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOGGER.warn("Check thread still doesn't exit after 60s");
          continuousQueryTaskSubmitThread.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }

      continuousQueryPlans.clear();

      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
    } catch (IOException e) {
      LOGGER.warn("Something wrong occurred While stopping CQService: {}", e.getMessage());
    }
  }

  private final ReentrantLock registrationLock = new ReentrantLock();

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  public boolean register(CreateContinuousQueryPlan plan, boolean shouldWriteLog)
      throws ContinuousQueryException {
    if (continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
      throw new ContinuousQueryException(
          String.format("Continuous Query [%s] already exists", plan.getContinuousQueryName()));
    }

    // if it is not processing recovery
    if (shouldWriteLog) {
      checkSchemaBeforeRegistration(plan);
    }

    acquireRegistrationLock();
    try {
      if (shouldWriteLog) {
        logWriter.createContinuousQuery(plan);
      }
      doRegister(plan);
      return true;
    } catch (Exception e) {
      throw new ContinuousQueryException(e.getMessage());
    } finally {
      releaseRegistrationLock();
    }
  }

  private void checkSchemaBeforeRegistration(CreateContinuousQueryPlan plan)
      throws ContinuousQueryException {
    try {
      new ContinuousQuerySchemaCheckTask(plan, plan.getFirstExecutionTimeBoundary()).run();
    } catch (Exception e) {
      throw new ContinuousQueryException("Failed to create continuous query task.", e);
    }
  }

  private void doRegister(CreateContinuousQueryPlan plan) {
    continuousQueryPlans.put(plan.getContinuousQueryName(), plan);
    nextExecutionTimestamps.put(
        plan.getContinuousQueryName(),
        calculateNextExecutionTimestamp(plan, DatetimeUtils.currentTime()));
  }

  @TestOnly
  public void deregisterAll() throws ContinuousQueryException {
    for (String cqName : continuousQueryPlans.keySet()) {
      deregister(new DropContinuousQueryPlan(cqName), false);
    }
  }

  public boolean deregister(DropContinuousQueryPlan plan, boolean shouldWriteLog)
      throws ContinuousQueryException {
    if (!continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
      throw new ContinuousQueryException(
          String.format("Continuous Query [%s] does not exist", plan.getContinuousQueryName()));
    }

    acquireRegistrationLock();
    try {
      if (shouldWriteLog) {
        logWriter.dropContinuousQuery(plan);
      }
      doDeregister(plan);
      return true;
    } catch (Exception e) {
      throw new ContinuousQueryException(e.getMessage());
    } finally {
      releaseRegistrationLock();
    }
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
              plan.getForInterval(),
              plan.getFirstExecutionTimeBoundary()));
    }
    return results;
  }

  private ContinuousQueryService() {}

  private static final ContinuousQueryService INSTANCE = new ContinuousQueryService();

  public static ContinuousQueryService getInstance() {
    return INSTANCE;
  }
}
