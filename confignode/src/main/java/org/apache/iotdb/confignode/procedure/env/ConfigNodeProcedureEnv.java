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

import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  private final ConfigManager configManager;

  private static boolean skipForTest = false;

  private static boolean invalidCacheResult = true;

  public static void setSkipForTest(boolean skipForTest) {
    ConfigNodeProcedureEnv.skipForTest = skipForTest;
  }

  public static void setInvalidCacheResult(boolean result) {
    ConfigNodeProcedureEnv.invalidCacheResult = result;
  }

  public ConfigNodeProcedureEnv(ConfigManager configManager) {
    this.configManager = configManager;
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
    DeleteStorageGroupReq deleteStorageGroupReq = new DeleteStorageGroupReq(name);
    return configManager.getClusterSchemaManager().deleteStorageGroup(deleteStorageGroupReq);
  }

  /**
   * Pre delete a storage group
   *
   * @param preDeleteType execute/rollback
   * @param deleteSgName storage group name
   */
  public void preDelete(PreDeleteStorageGroupReq.PreDeleteType preDeleteType, String deleteSgName) {
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
}
