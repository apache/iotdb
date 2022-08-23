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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.DeleteStorageGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.confignode.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.confignode.procedure.store.ConfigProcedureStore;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;
import org.apache.iotdb.confignode.procedure.store.ProcedureStore;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProcedureManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();

  private static final int procedureWaitTimeOut = 30;
  private static final int procedureWaitRetryTimeout = 250;

  private final ConfigManager configManager;
  private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
  private ProcedureScheduler scheduler;
  private IProcedureStore store;
  private ConfigNodeProcedureEnv env;

  public ProcedureManager(ConfigManager configManager, ProcedureInfo procedureInfo) {
    this.configManager = configManager;
    this.scheduler = new SimpleProcedureScheduler();
    this.store = new ConfigProcedureStore(configManager, procedureInfo);
    this.env = new ConfigNodeProcedureEnv(configManager, scheduler);
    this.executor = new ProcedureExecutor<>(env, store, scheduler);
  }

  public void shiftExecutor(boolean running) {
    if (running) {
      if (!executor.isRunning()) {
        executor.init(CONFIG_NODE_CONFIG.getProcedureCoreWorkerThreadsSize());
        executor.startWorkers();
        executor.startCompletedCleaner(
            CONFIG_NODE_CONFIG.getProcedureCompletedCleanInterval(),
            CONFIG_NODE_CONFIG.getProcedureCompletedEvictTTL());
        store.start();
        LOGGER.info("ProcedureManager is started successfully.");
      }
    } else {
      if (executor.isRunning()) {
        executor.stop();
        if (!executor.isRunning()) {
          executor.join();
          store.stop();
          LOGGER.info("ProcedureManager is stopped successfully.");
        }
      }
    }
  }

  public TSStatus deleteStorageGroups(ArrayList<TStorageGroupSchema> deleteSgSchemaList) {
    List<Long> procedureIds = new ArrayList<>();
    for (TStorageGroupSchema storageGroupSchema : deleteSgSchemaList) {
      DeleteStorageGroupProcedure deleteStorageGroupProcedure =
          new DeleteStorageGroupProcedure(storageGroupSchema);
      long procedureId = this.executor.submitProcedure(deleteStorageGroupProcedure);
      procedureIds.add(procedureId);
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed = waitingProcedureFinished(procedureIds, procedureStatus);
    // clear the previously deleted regions
    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
    partitionManager.getRegionCleaner().submit(partitionManager::clearDeletedRegions);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return RpcUtils.getStatus(procedureStatus);
    }
  }

  /** Generate a AddConfigNodeProcedure, and serially execute all the AddConfigNodeProcedure */
  public void addConfigNode(TConfigNodeRegisterReq req) {
    AddConfigNodeProcedure addConfigNodeProcedure =
        new AddConfigNodeProcedure(req.getConfigNodeLocation());
    this.executor.submitProcedure(addConfigNodeProcedure);
  }

  /**
   * Generate a RemoveConfigNodeProcedure, and serially execute all the RemoveConfigNodeProcedure
   */
  public void removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    RemoveConfigNodeProcedure removeConfigNodeProcedure =
        new RemoveConfigNodeProcedure(removeConfigNodePlan.getConfigNodeLocation());
    this.executor.submitProcedure(removeConfigNodeProcedure);
    LOGGER.info("Submit to remove ConfigNode, {}", removeConfigNodePlan);
  }

  /** Generate RemoveDataNodeProcedures, and serially execute all the RemoveDataNodeProcedure */
  public boolean removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    removeDataNodePlan
        .getDataNodeLocations()
        .forEach(
            tDataNodeLocation -> {
              this.executor.submitProcedure(new RemoveDataNodeProcedure(tDataNodeLocation));
              LOGGER.info("Submit to remove data node procedure, {}", tDataNodeLocation);
            });
    return true;
  }

  /**
   * Generate CreateRegionGroupsProcedure and wait for it finished
   *
   * @return SUCCESS_STATUS if all RegionGroups created successfully, CREATE_REGION_ERROR otherwise
   */
  public TSStatus createRegionGroups(CreateRegionGroupsPlan createRegionGroupsPlan) {
    long procedureId =
        executor.submitProcedure(new CreateRegionGroupsProcedure(createRegionGroupsPlan));
    List<TSStatus> statusList = new ArrayList<>();
    boolean isSucceed =
        waitingProcedureFinished(Collections.singletonList(procedureId), statusList);
    if (isSucceed) {
      return RpcUtils.SUCCESS_STATUS;
    } else {
      return new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())
          .setMessage(statusList.get(0).getMessage());
    }
  }

  /**
   * Waiting until the specific procedures finished
   *
   * @param procedureIds The specific procedures' index
   * @param statusList The corresponding running results of these procedures
   * @return True if all Procedures finished successfully, false otherwise
   */
  private boolean waitingProcedureFinished(List<Long> procedureIds, List<TSStatus> statusList) {
    boolean isSucceed = true;
    for (long procedureId : procedureIds) {
      long startTimeForCurrentProcedure = System.currentTimeMillis();
      while (executor.isRunning()
          && !executor.isFinished(procedureId)
          && TimeUnit.MILLISECONDS.toSeconds(
                  System.currentTimeMillis() - startTimeForCurrentProcedure)
              < procedureWaitTimeOut) {
        sleepWithoutInterrupt(procedureWaitRetryTimeout);
      }
      Procedure<ConfigNodeProcedureEnv> finishedProcedure =
          executor.getResultOrProcedure(procedureId);
      if (finishedProcedure.isSuccess()) {
        statusList.add(StatusUtils.OK);
      } else {
        statusList.add(
            StatusUtils.EXECUTE_STATEMENT_ERROR.setMessage(
                finishedProcedure.getException().getMessage()));
        isSucceed = false;
      }
    }
    return isSucceed;
  }

  public static void sleepWithoutInterrupt(final long timeToSleep) {
    long currentTime = System.currentTimeMillis();
    long endTime = timeToSleep + currentTime;
    boolean interrupted = false;
    while (currentTime < endTime) {
      try {
        Thread.sleep(endTime - currentTime);
      } catch (InterruptedException e) {
        interrupted = true;
      }
      currentTime = System.currentTimeMillis();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  // ======================================================
  /*
     GET-SET Region
  */
  // ======================================================
  public IManager getConfigManager() {
    return configManager;
  }

  public ProcedureExecutor<ConfigNodeProcedureEnv> getExecutor() {
    return executor;
  }

  public void setExecutor(ProcedureExecutor<ConfigNodeProcedureEnv> executor) {
    this.executor = executor;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public void setScheduler(ProcedureScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public IProcedureStore getStore() {
    return store;
  }

  public void setStore(ProcedureStore store) {
    this.store = store;
  }

  public ConfigNodeProcedureEnv getEnv() {
    return env;
  }

  public void setEnv(ConfigNodeProcedureEnv env) {
    this.env = env;
  }

  public void reportRegionMigrateResult(TRegionMigrateResultReportReq req) {
    LOGGER.info("receive DataNode region:{} migrate result:{}", req.getRegionId(), req);
    this.executor
        .getProcedures()
        .values()
        .forEach(
            procedure -> {
              if (procedure instanceof RegionMigrateProcedure) {
                RegionMigrateProcedure regionMigrateProcedure = (RegionMigrateProcedure) procedure;
                if (regionMigrateProcedure.getConsensusGroupId().equals(req.getRegionId())) {
                  regionMigrateProcedure.notifyTheRegionMigrateFinished(req);
                } else {
                  LOGGER.warn(
                      "DataNode report region:{} is not equals ConfigNode send region:{}",
                      req.getRegionId(),
                      regionMigrateProcedure.getConsensusGroupId());
                }
              }
            });
  }
}
