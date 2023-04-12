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
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quotas.SpaceQuotaType;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowSpaceResourceResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSpaceResourceResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQuotaManager.class);

  private final IManager configManager;
  private final QuotaInfo quotaInfo;
  private final Map<Integer, Long> deviceNum;
  private final Map<Integer, Long> timeSeriesNum;
  private final Map<String, List<Integer>> schemaRegionIdMap;
  private final Map<String, List<Integer>> dataRegionIdMap;
  private final Map<Integer, Long> regionDisk;

  public ClusterQuotaManager(IManager configManager, QuotaInfo quotaInfo) {
    this.configManager = configManager;
    this.quotaInfo = quotaInfo;
    deviceNum = new ConcurrentHashMap<>();
    timeSeriesNum = new ConcurrentHashMap<>();
    schemaRegionIdMap = new HashMap<>();
    dataRegionIdMap = new HashMap<>();
    regionDisk = new ConcurrentHashMap<>();
  }

  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    if (!checkSpaceQuota(req)) {
      return RpcUtils.getStatus(
          TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
          "The used quota exceeds the preset quota. Please set a larger value.");
    }
    ConsensusWriteResponse response =
        configManager
            .getConsensusManager()
            .write(new SetSpaceQuotaPlan(req.getDatabase(), req.getSpaceLimit()));
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
          req.getDatabase().toString(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  /** If the new quota is smaller than the quota already used, the setting fails. */
  private boolean checkSpaceQuota(TSetSpaceQuotaReq req) {
    for (String database : req.getDatabase()) {
      if (quotaInfo.getSpaceQuotaLimit().containsKey(database)) {
        TSpaceQuota spaceQuota = quotaInfo.getSpaceQuotaUsage().get(database);
        if (req.getSpaceLimit().getDeviceNum() != -1
            && spaceQuota.getDeviceNum() > req.getSpaceLimit().getDeviceNum()) {
          return false;
        }
        if (req.getSpaceLimit().getTimeserieNum() != -1
            && spaceQuota.getTimeserieNum() > req.getSpaceLimit().getTimeserieNum()) {
          return false;
        }
        if (req.getSpaceLimit().getDiskSize() != -1
            && spaceQuota.getDiskSize() > req.getSpaceLimit().getDiskSize()) {
          return false;
        }
      }
    }
    return true;
  }

  public TSpaceQuotaResp showSpaceQuota(List<String> databases) {
    TSpaceQuotaResp showSpaceQuotaResp = new TSpaceQuotaResp();
    if (databases.isEmpty()) {
      showSpaceQuotaResp.setSpaceQuota(quotaInfo.getSpaceQuotaLimit());
      showSpaceQuotaResp.setSpaceQuotaUsage(quotaInfo.getSpaceQuotaUsage());
    } else if (!quotaInfo.getSpaceQuotaLimit().isEmpty()) {
      Map<String, TSpaceQuota> spaceQuotaMap = new HashMap<>();
      Map<String, TSpaceQuota> spaceQuotaUsageMap = new HashMap<>();
      for (String database : databases) {
        if (quotaInfo.getSpaceQuotaLimit().containsKey(database)) {
          spaceQuotaMap.put(database, quotaInfo.getSpaceQuotaLimit().get(database));
          spaceQuotaUsageMap.put(database, quotaInfo.getSpaceQuotaUsage().get(database));
        }
      }
      showSpaceQuotaResp.setSpaceQuota(spaceQuotaMap);
      showSpaceQuotaResp.setSpaceQuotaUsage(spaceQuotaUsageMap);
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

  public List<Integer> getSchemaRegionIds() {
    List<Integer> schemaRegionIds = new ArrayList<>();
    getPartitionManager()
        .getSchemaRegionIds(
            new ArrayList<>(quotaInfo.getSpaceQuotaLimit().keySet()), schemaRegionIdMap);
    schemaRegionIdMap.values().forEach(schemaRegionIds::addAll);
    return schemaRegionIds;
  }

  public List<Integer> getDataRegionIds() {
    List<Integer> dataRegionIds = new ArrayList<>();
    getPartitionManager()
        .getDataRegionIds(
            new ArrayList<>(quotaInfo.getSpaceQuotaLimit().keySet()), dataRegionIdMap);
    dataRegionIdMap.values().forEach(dataRegionIds::addAll);
    return dataRegionIds;
  }

  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) {
    ConsensusWriteResponse response =
        configManager
            .getConsensusManager()
            .write(new SetThrottleQuotaPlan(req.getUserName(), req.getThrottleQuota()));
    if (response.getStatus() != null) {
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodeLocations();
        AsyncClientHandler<TSetThrottleQuotaReq, TSStatus> clientHandler =
            new AsyncClientHandler<>(
                DataNodeRequestType.SET_THROTTLE_QUOTA, req, dataNodeLocationMap);
        AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
        return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
      }
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while setting throttle quota on user: {}: ",
          req.getUserName(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) {
    TThrottleQuotaResp throttleQuotaResp = new TThrottleQuotaResp();
    if (req.getUserName() == null) {
      throttleQuotaResp.setThrottleQuota(quotaInfo.getThrottleQuotaLimit());
    } else {
      Map<String, TThrottleQuota> throttleLimit = new HashMap<>();
      throttleLimit.put(
          req.getUserName(),
          quotaInfo.getThrottleQuotaLimit().get(req.getUserName()) == null
              ? new TThrottleQuota()
              : quotaInfo.getThrottleQuotaLimit().get(req.getUserName()));
      throttleQuotaResp.setThrottleQuota(throttleLimit);
    }
    throttleQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return throttleQuotaResp;
  }

  public TThrottleQuotaResp getThrottleQuota() {
    TThrottleQuotaResp throttleQuotaResp = new TThrottleQuotaResp();
    if (!quotaInfo.getThrottleQuotaLimit().isEmpty()) {
      throttleQuotaResp.setThrottleQuota(quotaInfo.getThrottleQuotaLimit());
    }
    throttleQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return throttleQuotaResp;
  }

  public Map<String, TSpaceQuota> getSpaceQuotaUsage() {
    return quotaInfo.getSpaceQuotaUsage();
  }

  public Map<Integer, Long> getDeviceNum() {
    return deviceNum;
  }

  public Map<Integer, Long> getTimeSeriesNum() {
    return timeSeriesNum;
  }

  public Map<Integer, Long> getRegionDisk() {
    return regionDisk;
  }

  public void updateSpaceQuotaUsage() {
    AtomicLong deviceCount = new AtomicLong();
    AtomicLong timeSeriesCount = new AtomicLong();
    for (Map.Entry<String, List<Integer>> entry : schemaRegionIdMap.entrySet()) {
      deviceCount.set(0);
      timeSeriesCount.set(0);
      entry
          .getValue()
          .forEach(
              schemaRegionId -> {
                if (deviceNum.containsKey(schemaRegionId)) {
                  deviceCount.addAndGet(deviceCount.get() + deviceNum.get(schemaRegionId));
                }
                if (timeSeriesNum.containsKey(schemaRegionId)) {
                  timeSeriesCount.addAndGet(
                      timeSeriesCount.get() + timeSeriesNum.get(schemaRegionId));
                }
              });
      quotaInfo.getSpaceQuotaUsage().get(entry.getKey()).setDeviceNum(deviceCount.get());
      quotaInfo.getSpaceQuotaUsage().get(entry.getKey()).setTimeserieNum(timeSeriesCount.get());
    }
    AtomicLong regionDiskCount = new AtomicLong();
    for (Map.Entry<String, List<Integer>> entry : dataRegionIdMap.entrySet()) {
      regionDiskCount.set(0);
      entry
          .getValue()
          .forEach(
              dataRegionId -> {
                if (regionDisk.containsKey(dataRegionId)) {
                  regionDiskCount.addAndGet(regionDisk.get(dataRegionId));
                }
              });
      quotaInfo
          .getSpaceQuotaUsage()
          .get(entry.getKey())
          .setDiskSize(
              regionDiskCount.get()
                  * ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());
    }
  }

  public TShowSpaceResourceResp showSpaceResource() {
    TShowSpaceResourceResp showSpaceResourceResp = new TShowSpaceResourceResp();
    // get resource
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<Object, TSpaceResourceResp> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.GET_SPACE_RESOURCE, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    List<TSpaceResourceResp> responseList = clientHandler.getResponseList();
    long totalSpace = 0;
    long freeSpace = 0;
    long maxMemory = 0;
    for (TSpaceResourceResp spaceResourceResp : responseList) {
      if (spaceResourceResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        totalSpace += spaceResourceResp.getTotalSpace();
        freeSpace += spaceResourceResp.getFreeSpace();
        maxMemory += spaceResourceResp.getMaxMemory();
      }
    }
    Map<String, Map<String, Long>> spaceResource = new HashMap<>();
    Map<String, Long> resource = new HashMap<>();
    // Allocated
    long deviceNumAllocated = 0;
    long timeSeriesNumAllocated = 0;
    long diskSizeAllocated = 0;
    // Used
    long diskSizeUsed = 0;
    if (!quotaInfo.getSpaceQuotaLimit().isEmpty()) {
      deviceNumAllocated =
          quotaInfo.getSpaceQuotaLimit().values().stream()
              .mapToLong(TSpaceQuota::getDeviceNum)
              .sum();
      timeSeriesNumAllocated =
          quotaInfo.getSpaceQuotaLimit().values().stream()
              .mapToLong(TSpaceQuota::getTimeserieNum)
              .sum();
      // diskSize Unit : M
      diskSizeAllocated =
          quotaInfo.getSpaceQuotaLimit().values().stream().mapToLong(TSpaceQuota::getDiskSize).sum()
              / 1024;
      diskSizeUsed =
          quotaInfo.getSpaceQuotaUsage().values().stream().mapToLong(TSpaceQuota::getDiskSize).sum()
              / 1024;
    }

    // device num
    long deviceNum =
        maxMemory
            / 20
            / (180 + 2 * 100)
            / ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor();

    resource.put(IoTDBConstant.TOTAL, deviceNum);
    resource.put(IoTDBConstant.NON_USED, 0L);
    resource.put(IoTDBConstant.ALLOCATED, deviceNumAllocated);
    resource.put(IoTDBConstant.AVAILABLE, deviceNum - deviceNumAllocated);
    resource.put(IoTDBConstant.USED, 0L);
    spaceResource.put(SpaceQuotaType.deviceNum.name(), resource);

    // TimeSeries num
    resource = new HashMap<>();
    resource.put(IoTDBConstant.TOTAL, deviceNum);
    resource.put(IoTDBConstant.NON_USED, 0L);
    resource.put(IoTDBConstant.ALLOCATED, timeSeriesNumAllocated);
    resource.put(IoTDBConstant.AVAILABLE, deviceNum - timeSeriesNumAllocated);
    resource.put(IoTDBConstant.USED, 0L);
    spaceResource.put(SpaceQuotaType.timeSeriesNum.name(), resource);

    // Disk Size
    resource = new HashMap<>();
    // totalSpace, freeSpace Unit : B
    totalSpace = totalSpace / 1024 / 1024 / 1024;
    freeSpace = freeSpace / 1024 / 1024 / 1024;
    resource.put(IoTDBConstant.TOTAL, totalSpace);
    resource.put(IoTDBConstant.NON_USED, totalSpace - freeSpace - diskSizeUsed);
    resource.put(IoTDBConstant.ALLOCATED, diskSizeAllocated);
    resource.put(IoTDBConstant.AVAILABLE, freeSpace + diskSizeUsed - diskSizeAllocated);
    resource.put(IoTDBConstant.USED, diskSizeUsed);
    spaceResource.put(SpaceQuotaType.diskSize.name(), resource);

    showSpaceResourceResp.setSpaceResource(spaceResource);
    showSpaceResourceResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return showSpaceResourceResp;
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
