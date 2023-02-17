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
package org.apache.iotdb.confignode.manager.cq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.cq.CQState;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CQManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(CQManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  private final ReadWriteLock lock;

  private ScheduledExecutorService executor;

  public CQManager(ConfigManager configManager) {
    this.configManager = configManager;
    this.lock = new ReentrantReadWriteLock();
    this.executor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(CONF.getCqSubmitThread(), "CQ-Scheduler");
  }

  public TSStatus createCQ(TCreateCQReq req) {
    lock.readLock().lock();
    try {
      ScheduledExecutorService currentExecutor = executor;
      return configManager.getProcedureManager().createCQ(req, currentExecutor);
    } finally {
      lock.readLock().unlock();
    }
  }

  public TSStatus dropCQ(TDropCQReq req) {
    ConsensusWriteResponse response =
        configManager.getConsensusManager().write(new DropCQPlan(req.cqId));
    if (response.getStatus() != null) {
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while dropping cq {}: ", req.cqId, response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  public TShowCQResp showCQ() {
    ConsensusReadResponse response = configManager.getConsensusManager().read(new ShowCQPlan());
    if (response.getDataset() != null) {
      return ((ShowCQResp) response.getDataset()).convertToRpcShowCQResp();
    } else {
      LOGGER.warn("Unexpected error happened while showing cq: ", response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getException().toString());
      return new TShowCQResp(res, Collections.emptyList());
    }
  }

  public ScheduledExecutorService getExecutor() {
    ScheduledExecutorService res;
    lock.readLock().lock();
    try {
      res = executor;
    } finally {
      lock.readLock().unlock();
    }
    return res;
  }

  public void startCQScheduler() {
    lock.writeLock().lock();
    try {
      // 1. shutdown previous cq schedule thread pool
      try {
        if (executor != null) {
          executor.shutdown();
        }
      } catch (Throwable t) {
        // just print the error log because we should make sure we can start a new cq schedule pool
        // successfully in the next steps
        LOGGER.error("Error happened while shutting down previous cq schedule thread pool.", t);
      }

      // 2. start a new schedule thread pool
      executor =
          IoTDBThreadPoolFactory.newScheduledThreadPool(CONF.getCqSubmitThread(), "CQ-Scheduler");

      // 3. get all CQs
      List<CQInfo.CQEntry> allCQs = null;
      // wait for consensus layer ready
      while (configManager.getConsensusManager() == null) {
        try {
          LOGGER.info("consensus layer is not ready, sleep 1s...");
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("Unexpected interruption during waiting for consensus layer ready.");
        }
      }
      // keep fetching until we get all CQEntries if this node is still leader
      while (allCQs == null && configManager.getConsensusManager().isLeader()) {
        ConsensusReadResponse response = configManager.getConsensusManager().read(new ShowCQPlan());
        if (response.getDataset() != null) {
          allCQs = ((ShowCQResp) response.getDataset()).getCqList();
        } else {
          // consensus layer related errors
          LOGGER.warn(
              "Unexpected error happened while fetching cq list: ", response.getException());
        }
      }

      // 4. recover the scheduling of active CQs
      if (allCQs != null) {
        for (CQInfo.CQEntry entry : allCQs) {
          if (entry.getState() == CQState.ACTIVE) {
            CQScheduleTask cqScheduleTask = new CQScheduleTask(entry, executor, configManager);
            cqScheduleTask.submitSelf();
          }
        }
      }

    } finally {
      lock.writeLock().unlock();
    }
  }

  public void stopCQScheduler() {
    ScheduledExecutorService previous;
    lock.writeLock().lock();
    try {
      previous = executor;
      executor = null;
    } finally {
      lock.writeLock().unlock();
    }
    if (previous != null) {
      previous.shutdown();
    }
  }
}
