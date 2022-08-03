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
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.AddConfigNodeProcedure;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    List<Long> procIdList = new ArrayList<>();
    for (TStorageGroupSchema storageGroupSchema : deleteSgSchemaList) {
      DeleteStorageGroupProcedure deleteStorageGroupProcedure =
          new DeleteStorageGroupProcedure(storageGroupSchema);
      long procId = this.executor.submitProcedure(deleteStorageGroupProcedure);
      procIdList.add(procId);
    }
    List<TSStatus> procedureStatus = new ArrayList<>();
    boolean isSucceed = getProcedureStatus(this.executor, procIdList, procedureStatus);
    // clear the previously deleted regions
    final PartitionManager partitionManager = getConfigManager().getPartitionManager();
    partitionManager.getRegionCleaner().submit(partitionManager::clearDeletedRegions);
    if (isSucceed) {
      return StatusUtils.OK;
    } else {
      return RpcUtils.getStatus(procedureStatus);
    }
  }

  /**
   * generate a procedure, and execute by one by one
   *
   * @param req new config node
   */
  public void addConfigNode(TConfigNodeRegisterReq req) {
    AddConfigNodeProcedure addConfigNodeProcedure =
        new AddConfigNodeProcedure(req.getConfigNodeLocation());
    this.executor.submitProcedure(addConfigNodeProcedure);
  }

  /**
   * generate a procedure, and execute remove confignode one by one
   *
   * @param removeConfigNodePlan remove config node plan
   */
  public void removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    RemoveConfigNodeProcedure removeConfigNodeProcedure =
        new RemoveConfigNodeProcedure(removeConfigNodePlan.getConfigNodeLocation());
    this.executor.submitProcedure(removeConfigNodeProcedure);
    LOGGER.info("Submit to remove ConfigNode, {}", removeConfigNodePlan);
  }

  /**
   * generate a procedure, and execute remove datanode one by one
   *
   * @param removeDataNodePlan
   * @return
   */
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

  private static boolean getProcedureStatus(
      ProcedureExecutor executor, List<Long> procIds, List<TSStatus> statusList) {
    boolean isSucceed = true;
    for (long procId : procIds) {
      long startTimeForProcId = System.currentTimeMillis();
      while (executor.isRunning()
          && !executor.isFinished(procId)
          && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTimeForProcId)
              < procedureWaitTimeOut) {
        sleepWithoutInterrupt(procedureWaitRetryTimeout);
      }
      Procedure finishedProc = executor.getResultOrProcedure(procId);
      if (finishedProc.isSuccess()) {
        statusList.add(StatusUtils.OK);
      } else {
        statusList.add(
            StatusUtils.EXECUTE_STATEMENT_ERROR.setMessage(
                finishedProc.getException().getMessage()));
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
    this.executor.getProcedures().values().stream()
        .forEach(
            procedure -> {
              if (procedure instanceof RegionMigrateProcedure) {
                RegionMigrateProcedure regionMigrateProcedure = (RegionMigrateProcedure) procedure;
                if (regionMigrateProcedure.getConsensusGroupId().equals(req.getRegionId())) {
                  regionMigrateProcedure.notifyTheRegionMigrateFinished(req);
                }
              }
            });
  }
}
