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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  private final ReentrantLock addConfigNodeLock = new ReentrantLock();

  private final ConfigManager configManager;

  private final ProcedureScheduler scheduler;

  private static boolean skipForTest = false;

  private static boolean invalidCacheResult = true;

  public static void setSkipForTest(boolean skipForTest) {
    ConfigNodeProcedureEnv.skipForTest = skipForTest;
  }

  public static void setInvalidCacheResult(boolean result) {
    ConfigNodeProcedureEnv.invalidCacheResult = result;
  }

  public ConfigNodeProcedureEnv(ConfigManager configManager, ProcedureScheduler scheduler) {
    this.configManager = configManager;
    this.scheduler = scheduler;
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Delete ConfigNode cache, includes ClusterSchemaInfo and PartitionInfo
   *
   * @param name storage group name
   * @return tsStatus
   */
  public TSStatus deleteConfig(String name) {
    DeleteStorageGroupPlan deleteStorageGroupPlan = new DeleteStorageGroupPlan(name);
    return configManager.getClusterSchemaManager().deleteStorageGroup(deleteStorageGroupPlan);
  }

  /**
   * Pre delete a storage group
   *
   * @param preDeleteType execute/rollback
   * @param deleteSgName storage group name
   */
  public void preDelete(
      PreDeleteStorageGroupPlan.PreDeleteType preDeleteType, String deleteSgName) {
    configManager.getPartitionManager().preDeleteStorageGroup(deleteSgName, preDeleteType);
  }

  /**
   * @param storageGroupName Storage group name
   * @return ALL SUCCESS OR NOT
   * @throws IOException IOE
   * @throws TException Thrift IOE
   */
  public boolean invalidateCache(String storageGroupName) throws IOException, TException {
    // TODO: Remove it after IT is supported
    if (skipForTest) {
      return invalidCacheResult;
    }
    List<TDataNodeInfo> allDataNodes = configManager.getNodeManager().getOnlineDataNodes(-1);
    TInvalidateCacheReq invalidateCacheReq = new TInvalidateCacheReq();
    invalidateCacheReq.setStorageGroup(true);
    invalidateCacheReq.setFullPath(storageGroupName);
    for (TDataNodeInfo dataNodeInfo : allDataNodes) {
      final TSStatus invalidateSchemaStatus =
          SyncDataNodeClientPool.getInstance()
              .invalidateSchemaCache(
                  dataNodeInfo.getLocation().getInternalEndPoint(), invalidateCacheReq);
      final TSStatus invalidatePartitionStatus =
          SyncDataNodeClientPool.getInstance()
              .invalidatePartitionCache(
                  dataNodeInfo.getLocation().getInternalEndPoint(), invalidateCacheReq);
      if (!verifySucceed(invalidatePartitionStatus, invalidateSchemaStatus)) {
        LOG.error(
            "Invalidate cache failed, invalidate partition cache status is {}， invalidate schema cache status is {}",
            invalidatePartitionStatus,
            invalidateSchemaStatus);
        return false;
      }
    }
    return true;
  }

  public boolean verifySucceed(TSStatus... status) {
    return Arrays.stream(status)
        .allMatch(tsStatus -> tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Let the remotely new ConfigNode build the ConsensusGroup
   *
   * @param tConfigNodeLocation New ConfigNode's location
   */
  public void addConsensusGroup(TConfigNodeLocation tConfigNodeLocation) throws Exception {
    List<TConfigNodeLocation> configNodeLocations =
        new ArrayList<>(configManager.getNodeManager().getRegisteredConfigNodes());
    configNodeLocations.add(tConfigNodeLocation);
    SyncConfigNodeClientPool.getInstance()
        .addConsensusGroup(tConfigNodeLocation.getInternalEndPoint(), configNodeLocations);
  }

  /**
   * Leader will add the new ConfigNode Peer into PartitionRegion
   *
   * @param configNodeLocation The new ConfigNode
   * @throws AddPeerException When addPeer doesn't success
   */
  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
    configManager.getConsensusManager().addConfigNodePeer(configNodeLocation);
  }

  /**
   * Leader will record the new ConfigNode's information
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void applyConfigNode(TConfigNodeLocation configNodeLocation) {
    configManager.getNodeManager().applyConfigNode(configNodeLocation);
  }

  /**
   * Leader will notify the new ConfigNode that registration success
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void notifyRegisterSuccess(TConfigNodeLocation configNodeLocation) {
    SyncConfigNodeClientPool.getInstance()
        .notifyRegisterSuccess(configNodeLocation.getInternalEndPoint());
  }

  public ReentrantLock getAddConfigNodeLock() {
    return addConfigNodeLock;
  }

  public ProcedureScheduler getScheduler() {
    return scheduler;
  }
}
