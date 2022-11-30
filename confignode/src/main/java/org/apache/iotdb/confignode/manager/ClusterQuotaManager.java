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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: Manage quotas for storage groups
public class ClusterQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQuotaManager.class);

  private final IManager configManager;
  private final QuotaInfo quotaInfo;
  private final Map<Integer, Integer> deviceNum;
  private final Map<String, List<Integer>> schemaIdMap;

  public ClusterQuotaManager(IManager configManager, QuotaInfo quotaInfo) {
    this.configManager = configManager;
    this.quotaInfo = quotaInfo;
    deviceNum = new HashMap<>();
    schemaIdMap = new HashMap<>();
  }

  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    ConsensusWriteResponse response =
        configManager
            .getConsensusManager()
            .write(new SetSpaceQuotaPlan(req.getStorageGroup(), req.getSpaceLimit()));
    if (response.getStatus() != null) {
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodeLocations();
        AsyncClientHandler<TSetSpaceQuotaReq, TSStatus> clientHandler =
            new AsyncClientHandler<>(DataNodeRequestType.SET_SPACE_QUOTA, req, dataNodeLocationMap);
        AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
        return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
      }
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while setting space quota on {}: ",
          req.getStorageGroup().toString(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  public TSpaceQuotaResp showSpaceQuota(List<String> storageGroups) {
    TSpaceQuotaResp showSpaceQuotaResp = new TSpaceQuotaResp();
    if (storageGroups.isEmpty()) {
      showSpaceQuotaResp.setSpaceQuota(quotaInfo.getSpaceQuotaLimit());
    } else if (!quotaInfo.getSpaceQuotaLimit().isEmpty()) {
      Map<String, TSpaceQuota> spaceQuotaMap = new HashMap<>();
      for (String storageGroup : storageGroups) {
        if (quotaInfo.getSpaceQuotaLimit().containsKey(storageGroup)) {
          spaceQuotaMap.put(storageGroup, quotaInfo.getSpaceQuotaLimit().get(storageGroup));
        }
      }
      showSpaceQuotaResp.setSpaceQuota(spaceQuotaMap);
    }
    showSpaceQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return showSpaceQuotaResp;
  }

  public TSpaceQuotaResp getSpaceQuota() {
    TSpaceQuotaResp spaceQuotaResp = new TSpaceQuotaResp();
    if (!quotaInfo.getSpaceQuotaLimit().isEmpty()) {
      spaceQuotaResp.setSpaceQuota(quotaInfo.getSpaceQuotaLimit());
    }
    spaceQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return spaceQuotaResp;
  }

  public boolean hasSpaceQuotaLimit() {
    return quotaInfo.getSpaceQuotaLimit().keySet().isEmpty();
  }

  public List<Integer> getSchemaIds() {
    List<Integer> schemaIds = new ArrayList<>();
    getPartitionManager()
        .getSchemaIds(new ArrayList<>(quotaInfo.getSpaceQuotaLimit().keySet()), schemaIdMap);
    schemaIdMap.values().forEach(schemaIdList -> schemaIds.addAll(schemaIdList));
    return schemaIds;
  }

  public Map<String, TSpaceQuota> getUseSpaceQuota() {
    return quotaInfo.getUseSpaceQuota();
  }

  public Map<Integer, Integer> getDeviceNum() {
    return deviceNum;
  }

  public void updateDeviceNum() {
    AtomicInteger deviceCount = new AtomicInteger();
    for (Map.Entry<String, List<Integer>> entry : schemaIdMap.entrySet()) {
      deviceCount.set(0);
      entry
          .getValue()
          .forEach(
              schemaId -> {
                if (deviceNum.containsKey(schemaId)) {
                  deviceCount.addAndGet(deviceCount.get() + deviceNum.get(schemaId));
                }
              });
      quotaInfo.getUseSpaceQuota().get(entry.getKey()).setDeviceNum(deviceCount.get());
    }
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
