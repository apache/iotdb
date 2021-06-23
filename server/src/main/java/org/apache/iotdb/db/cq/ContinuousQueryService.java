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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ContinuousQueryAlreadyExistException;
import org.apache.iotdb.db.exception.ContinuousQueryNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

public class ContinuousQueryService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(ContinuousQueryService.class);

  private final HashMap<String, ScheduledFuture<?>> continuousQueriesFutures = new HashMap<>();

  private final HashMap<String, CreateContinuousQueryPlan> continuousQueryPlans = new HashMap<>();

  private final ReentrantLock registrationLock = new ReentrantLock();

  private static final ContinuousQueryService INSTANCE = new ContinuousQueryService();

  private ScheduledExecutorService pool;

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
    pool =
        IoTDBThreadPoolFactory.newScheduledThreadPool(
            IoTDBDescriptor.getInstance().getConfig().getContinuousQueryThreadNum(),
            "Continuous Query Service");
    logger.info("Continuous query service started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      pool.shutdownNow();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    for (ScheduledFuture<?> future : continuousQueriesFutures.values()) {
      future.cancel(false);
    }
    logger.info("Waiting for task pool to shut down");
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      logger.info("Thread interrupted");
      Thread.currentThread().interrupt();
    }
    if (pool != null) {
      pool.shutdownNow();
    }
  }

  public boolean register(CreateContinuousQueryPlan plan, boolean writeLog)
      throws ContinuousQueryAlreadyExistException {

    acquireRegistrationLock();

    if (continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
      throw new ContinuousQueryAlreadyExistException(plan.getContinuousQueryName());
    }

    if (writeLog) {
      try {
        IoTDB.metaManager.createContinuousQuery(plan);
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }

    doRegister(plan);

    releaseRegistrationLock();
    return true;
  }

  private void doRegister(CreateContinuousQueryPlan plan) {

    try {
      ContinuousQueryTask cq = new ContinuousQueryTask(plan);
      ScheduledFuture<?> future =
          pool.scheduleAtFixedRate(
              cq,
              0,
              plan.getEveryInterval(),
              DatetimeUtils.toTimeUnit(
                  IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision()));
      continuousQueriesFutures.put(plan.getContinuousQueryName(), future);
      continuousQueryPlans.put(plan.getContinuousQueryName(), plan);
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  public void deregisterAll() throws ContinuousQueryNotExistException {
    for (String cqName : continuousQueryPlans.keySet()) {
      deregister(new DropContinuousQueryPlan(cqName));
    }
  }

  public boolean deregister(DropContinuousQueryPlan plan) throws ContinuousQueryNotExistException {

    acquireRegistrationLock();

    if (!continuousQueryPlans.containsKey(plan.getContinuousQueryName())) {
      releaseRegistrationLock();
      throw new ContinuousQueryNotExistException(plan.getContinuousQueryName());
    }

    try {

      IoTDB.metaManager.dropContinuousQuery(plan);
      doDeregister(plan);
    } catch (Exception e) {

      logger.error(e.getMessage());
    }

    releaseRegistrationLock();

    return true;
  }

  private void doDeregister(DropContinuousQueryPlan plan) {
    String cqName = plan.getContinuousQueryName();
    continuousQueriesFutures.get(cqName).cancel(false);
    continuousQueriesFutures.remove(cqName);
    continuousQueryPlans.remove(cqName);
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
