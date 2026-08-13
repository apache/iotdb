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
import org.apache.iotdb.common.rpc.thrift.TSetUserResourceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.UserResourceQuotaConverter;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.quota.DeleteUserResourceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetUserResourceQuotaPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowUserResourceQuotaReq;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResourceQuotaResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQuotaManager.class);

  private static final TUserResourceQuota EMPTY_USER_RESOURCE_QUOTA = buildEmptyUserResourceQuota();

  private final IManager configManager;
  private final QuotaInfo quotaInfo;
  private final Map<Integer, Long> deviceNum;
  private final Map<Integer, Long> timeSeriesNum;
  private final Map<String, List<Integer>> schemaRegionIdMap;
  private final Map<String, List<Integer>> dataRegionIdMap;
  private final Map<Integer, Long> regionDisk;

  /** dataNodeId -> latest user-resource in-use snapshot from heartbeat. */
  private final Map<Integer, TUserResourceUsageSnapshot> userResourceUsageByNode;

  public ClusterQuotaManager(IManager configManager, QuotaInfo quotaInfo) {
    this.configManager = configManager;
    this.quotaInfo = quotaInfo;
    deviceNum = new ConcurrentHashMap<>();
    timeSeriesNum = new ConcurrentHashMap<>();
    schemaRegionIdMap = new HashMap<>();
    dataRegionIdMap = new HashMap<>();
    regionDisk = new ConcurrentHashMap<>();
    userResourceUsageByNode = new ConcurrentHashMap<>();
  }

  public TSStatus setSpaceQuota(final TSetSpaceQuotaReq req) {
    if (!checkSpaceQuota(req)) {
      return RpcUtils.getStatus(
          TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
          "The used quota exceeds the preset quota. Please set a larger value.");
    }
    // TODO: Datanode failed to receive rpc
    try {
      final TSStatus response =
          configManager
              .getConsensusManager()
              .write(new SetSpaceQuotaPlan(req.getDatabase(), req.getSpaceLimit()));
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodeLocations();
        DataNodeAsyncRequestContext<TSetSpaceQuotaReq, TSStatus> clientHandler =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.SET_SPACE_QUOTA, req, dataNodeLocationMap);
        CnToDnInternalServiceAsyncRequestManager.getInstance()
            .sendAsyncRequestWithRetry(clientHandler);
        return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
      }
      return response;
    } catch (final ConsensusException e) {
      LOGGER.warn(
          String.format(
              ManagerMessages
                  .LOG_UNEXPECTED_ERROR_HAPPENED_SETTING_SPACE_QUOTA_DATABASE_ARG_F6ED7586,
              req.getDatabase()),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  /** If the new quota is smaller than the quota already used, the setting fails. */
  private boolean checkSpaceQuota(TSetSpaceQuotaReq req) {
    for (String database : req.getDatabase()) {
      TSpaceQuota spaceQuota = quotaInfo.getSpaceQuotaUsage().get(database);
      if (Objects.nonNull(spaceQuota)) {
        if (req.getSpaceLimit().getDeviceNum() != IoTDBConstant.UNLIMITED_VALUE
            && req.getSpaceLimit().getDeviceNum() != IoTDBConstant.DEFAULT_VALUE
            && spaceQuota.getDeviceNum() > req.getSpaceLimit().getDeviceNum()) {
          return false;
        }
        if (req.getSpaceLimit().getTimeserieNum() != IoTDBConstant.UNLIMITED_VALUE
            && req.getSpaceLimit().getTimeserieNum() != IoTDBConstant.DEFAULT_VALUE
            && spaceQuota.getTimeserieNum() > req.getSpaceLimit().getTimeserieNum()) {
          return false;
        }
        if (req.getSpaceLimit().getDiskSize() != IoTDBConstant.UNLIMITED_VALUE
            && req.getSpaceLimit().getDiskSize() != IoTDBConstant.DEFAULT_VALUE
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

  /**
   * Persist throttle by userId and broadcast to DataNodes; on TimechoDB, also drain pending
   * throttle→USER QUOTA migration.
   */
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) {
    long userId = resolveUserId(req.getUserName());
    if (userId < 0) {
      return RpcUtils.getStatus(
          TSStatusCode.USER_NOT_EXIST,
          String.format(ConfigNodeMessages.EXCEPTION_NO_SUCH_USER_ARG_D11B1046, req.getUserName()));
    }
    try {
      TSStatus response =
          configManager
              .getConsensusManager()
              .write(new SetThrottleQuotaPlan(req.getUserName(), userId, req.getThrottleQuota()));
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // TimechoDB only: mirror throttle into the userId-keyed user resource quota map.
        if (EditionGate.isUserResourceQuotaEnabled()) {
          ensureLegacyThrottleMigrated();
        }
        req.setUserId(userId);
        Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodeLocations();
        DataNodeAsyncRequestContext<TSetThrottleQuotaReq, TSStatus> clientHandler =
            new DataNodeAsyncRequestContext<>(
                CnToDnAsyncRequestType.SET_THROTTLE_QUOTA, req, dataNodeLocationMap);
        CnToDnInternalServiceAsyncRequestManager.getInstance()
            .sendAsyncRequestWithRetry(clientHandler);
        return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
      }
      return response;
    } catch (ConsensusException e) {
      LOGGER.warn(
          String.format(
              ManagerMessages
                  .LOG_UNEXPECTED_ERROR_HAPPENED_SETTING_THROTTLE_QUOTA_USER_ARG_C111BE81,
              req.getUserName()),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) {
    quotaInfo.upgradeLegacyThrottleKeys(this::resolveUserId);
    TThrottleQuotaResp throttleQuotaResp = new TThrottleQuotaResp();
    Map<Long, TThrottleQuota> throttleLimit = new HashMap<>();
    if (req.getUserName() == null) {
      throttleLimit.putAll(quotaInfo.getThrottleQuotaLimit());
    } else {
      long userId = resolveUserId(req.getUserName());
      if (userId >= 0) {
        TThrottleQuota quota = quotaInfo.getThrottleQuotaLimit().get(userId);
        throttleLimit.put(userId, quota == null ? new TThrottleQuota() : quota);
      }
    }
    Map<Long, String> userNameMap = buildThrottleUserNameMap(throttleLimit.keySet());
    throttleQuotaResp.setThrottleQuotaByUserId(throttleLimit);
    throttleQuotaResp.setThrottleQuota(buildLegacyThrottleQuotaMap(throttleLimit, userNameMap));
    throttleQuotaResp.setUserNameMap(userNameMap);
    throttleQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return throttleQuotaResp;
  }

  public TThrottleQuotaResp getThrottleQuota() {
    quotaInfo.upgradeLegacyThrottleKeys(this::resolveUserId);
    TThrottleQuotaResp throttleQuotaResp = new TThrottleQuotaResp();
    if (!quotaInfo.getThrottleQuotaLimit().isEmpty()) {
      Map<Long, TThrottleQuota> throttleLimit = quotaInfo.getThrottleQuotaLimit();
      Map<Long, String> userNameMap = buildThrottleUserNameMap(throttleLimit.keySet());
      throttleQuotaResp.setThrottleQuotaByUserId(throttleLimit);
      throttleQuotaResp.setThrottleQuota(buildLegacyThrottleQuotaMap(throttleLimit, userNameMap));
      throttleQuotaResp.setUserNameMap(userNameMap);
    }
    throttleQuotaResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return throttleQuotaResp;
  }

  /**
   * Resolve userId, persist SET/DELETE USER QUOTA, then broadcast the merged ConfigNode quota (not
   * the partial request).
   */
  public TSStatus setUserResourceQuota(TSetUserResourceQuotaReq req) {
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      return RpcUtils.getStatus(
          TSStatusCode.UNSUPPORTED_OPERATION,
          ConfigNodeMessages
              .EXCEPTION_USER_RESOURCE_QUOTA_IS_NOT_AVAILABLE_IN_THIS_EDITION_907835C0);
    }
    ensureLegacyThrottleMigrated();
    long userId = resolveUserId(req.getUserName());
    if (userId < 0) {
      return RpcUtils.getStatus(
          TSStatusCode.USER_NOT_EXIST,
          String.format(
              org.apache.iotdb.confignode.i18n.ConfigNodeMessages
                  .EXCEPTION_NO_SUCH_USER_ARG_D11B1046,
              req.getUserName()));
    }
    if (UserResourceQuotaConverter.isClearRequest(req.getUserResourceQuota())) {
      return deleteUserResourceQuota(userId, req.getUserName());
    }
    try {
      TSStatus response =
          configManager
              .getConsensusManager()
              .write(
                  new SetUserResourceQuotaPlan(
                      req.getUserName(), userId, req.getUserResourceQuota()));
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Broadcast the merged quota persisted on ConfigNode, not the partial request.
        TUserResourceQuota merged = quotaInfo.getUserResourceQuotaLimit().get(userId);
        return broadcastUserResourceQuota(
            userId, req.getUserName(), merged != null ? merged : req.getUserResourceQuota());
      }
      return response;
    } catch (ConsensusException e) {
      LOGGER.warn(
          ManagerMessages
              .LOG_UNEXPECTED_ERROR_HAPPENED_WHEN_SETTING_USER_RESOURCE_QUOTA_ARG_9BB8F473,
          e.getMessage(),
          e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  /** Resolve current userId by userName; returns -1 when the user does not exist. */
  private long resolveUserId(String userName) {
    try {
      org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp resp =
          configManager.getPermissionManager().getUser(userName);
      if (resp.getStatus() != null
          && resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && resp.getUserInfo() != null) {
        return resp.getUserInfo().getUserId();
      }
      return -1;
    } catch (Exception e) {
      LOGGER.warn(
          ManagerMessages.LOG_FAILED_TO_RESOLVE_USERID_FOR_USERNAME_ARG_900382A8, userName, e);
      return -1;
    }
  }

  /**
   * Replay pending throttle entries as userId-keyed SetUserResourceQuotaPlan writes, then broadcast
   * them to DataNodes. Only callable on the leader; the pending set itself is drained inside the
   * state machine so all replicas stay consistent.
   */
  public void ensureLegacyThrottleMigrated() {
    ensureLegacyThrottleMigrated(true);
  }

  /** Drain pending throttle→USER QUOTA migrations; optionally broadcast merged quotas to DNs. */
  private void ensureLegacyThrottleMigrated(boolean broadcast) {
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      return;
    }
    quotaInfo.upgradeLegacyThrottleKeys(this::resolveUserId);
    if (quotaInfo.getPendingThrottleMigration().isEmpty()) {
      return;
    }
    for (Long userId : new ArrayList<>(quotaInfo.getPendingThrottleMigration())) {
      if (userId == null || userId < 0 || userId == IoTDBConstant.SUPER_USER_ID) {
        quotaInfo.getPendingThrottleMigration().remove(userId);
        continue;
      }
      TThrottleQuota throttle = quotaInfo.getThrottleQuotaLimit().get(userId);
      if (throttle == null) {
        quotaInfo.getPendingThrottleMigration().remove(userId);
        continue;
      }
      String userName = resolveUserName(userId);
      TUserResourceQuota derived =
          UserResourceQuotaConverter.toThrift(
              UserResourceQuotaConverter.fromThrottleQuota(throttle));
      if (UserResourceQuotaConverter.isClearRequest(derived)) {
        quotaInfo.getPendingThrottleMigration().remove(userId);
        continue;
      }
      try {
        TSStatus response =
            configManager
                .getConsensusManager()
                .write(new SetUserResourceQuotaPlan(userName, userId, derived));
        if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          TUserResourceQuota merged = quotaInfo.getUserResourceQuotaLimit().get(userId);
          if (broadcast) {
            broadcastUserResourceQuota(userId, userName, merged != null ? merged : derived);
          }
        }
      } catch (ConsensusException e) {
        LOGGER.warn(
            ManagerMessages
                .LOG_UNEXPECTED_ERROR_HAPPENED_WHEN_SETTING_USER_RESOURCE_QUOTA_ARG_9BB8F473,
            e.getMessage(),
            e);
      }
    }
  }

  private TSStatus broadcastUserResourceQuota(
      long userId, String userName, TUserResourceQuota quota) {
    TSetUserResourceQuotaReq broadcastReq = new TSetUserResourceQuotaReq();
    broadcastReq.setUserName(userName);
    broadcastReq.setUserId(userId);
    broadcastReq.setUserResourceQuota(quota);
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TSetUserResourceQuotaReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.SET_USER_RESOURCE_QUOTA, broadcastReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
  }

  /** Broadcast quota cleanup to DataNodes after a user is dropped (state machine cascade done). */
  public void onUserDropped(long userId, String userName) {
    // Always clear DN throttle (both editions) and user-resource quota (TimechoDB).
    TSetThrottleQuotaReq throttleReq = new TSetThrottleQuotaReq();
    throttleReq.setUserName(userName == null ? "" : userName);
    throttleReq.setUserId(userId);
    TThrottleQuota emptyThrottle = new TThrottleQuota();
    emptyThrottle.setThrottleLimit(Collections.emptyMap());
    emptyThrottle.setMemLimit(0);
    emptyThrottle.setCpuLimit(0);
    throttleReq.setThrottleQuota(emptyThrottle);
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TSetThrottleQuotaReq, TSStatus> throttleHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.SET_THROTTLE_QUOTA, throttleReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(throttleHandler);
    if (EditionGate.isUserResourceQuotaEnabled()) {
      broadcastUserResourceQuota(userId, userName, EMPTY_USER_RESOURCE_QUOTA);
    }
  }

  private static TUserResourceQuota buildEmptyUserResourceQuota() {
    TUserResourceQuota empty = new TUserResourceQuota();
    empty.setReadQuota(Collections.emptyMap());
    empty.setWriteQuota(Collections.emptyMap());
    empty.setThrottleLimit(Collections.emptyMap());
    return empty;
  }

  /**
   * DELETE USER QUOTA: ConfigNode consensus is the source of truth. DataNodes converge via
   * reportUserResourceUsage sync when broadcast fails after a successful consensus write.
   */
  public TSStatus deleteUserResourceQuota(long userId, String userName) {
    try {
      TSStatus response =
          configManager
              .getConsensusManager()
              .write(new DeleteUserResourceQuotaPlan(userId, userName));
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Empty quota means clear on DataNode (see UserResourceQuotaConverter.isClearRequest).
        TSStatus broadcastStatus =
            broadcastUserResourceQuota(userId, userName, EMPTY_USER_RESOURCE_QUOTA);
        if (broadcastStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              ManagerMessages
                  .LOG_UNEXPECTED_ERROR_HAPPENED_WHEN_SETTING_USER_RESOURCE_QUOTA_ARG_9BB8F473,
              broadcastStatus.getMessage());
        }
        return broadcastStatus;
      }
      return response;
    } catch (ConsensusException e) {
      LOGGER.warn(
          ManagerMessages
              .LOG_UNEXPECTED_ERROR_HAPPENED_WHEN_SETTING_USER_RESOURCE_QUOTA_ARG_9BB8F473,
          e.getMessage(),
          e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  /** Resolve quotas by name/all users and attach per-Running-DataNode usage snapshots. */
  public TUserResourceQuotaResp showUserResourceQuota(TShowUserResourceQuotaReq req) {
    TUserResourceQuotaResp resp = new TUserResourceQuotaResp();
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      resp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION,
              ConfigNodeMessages
                  .EXCEPTION_USER_RESOURCE_QUOTA_IS_NOT_AVAILABLE_IN_THIS_EDITION_907835C0));
      return resp;
    }
    ensureLegacyThrottleMigrated();
    Map<Long, TUserResourceQuota> quotaMap = new HashMap<>();
    if (req.getUserName() == null) {
      quotaMap.putAll(quotaInfo.getUserResourceQuotaLimit());
    } else {
      long userId = resolveUserId(req.getUserName());
      TUserResourceQuota quota =
          userId < 0 ? null : quotaInfo.getUserResourceQuotaLimit().get(userId);
      if (quota != null) {
        quotaMap.put(userId, quota);
      }
    }
    resp.setUserResourceQuota(quotaMap);
    resp.setUserNameMap(buildUserNameMap(quotaMap.keySet()));
    // Aggregate usage for all Running DataNodes (reportUserResourceUsage cache).
    Map<Integer, TUserResourceUsageSnapshot> usage = new HashMap<>();
    try {
      for (org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration dn :
          configManager
              .getNodeManager()
              .filterDataNodeThroughStatus(org.apache.iotdb.commons.cluster.NodeStatus.Running)) {
        int nodeId = dn.getLocation().getDataNodeId();
        usage.put(
            nodeId, userResourceUsageByNode.getOrDefault(nodeId, new TUserResourceUsageSnapshot()));
      }
    } catch (Exception e) {
      LOGGER.warn(
          ManagerMessages
              .LOG_FAILED_TO_AGGREGATE_RUNNING_DATANODE_USAGE_FOR_SHOW_USER_QUOTA_ARG_DB436DC5,
          e.getMessage(),
          e);
    }
    if (!usage.isEmpty()) {
      resp.setUsageByDataNode(usage);
    }
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }

  public TUserResourceQuotaResp getUserResourceQuota() {
    TUserResourceQuotaResp resp = new TUserResourceQuotaResp();
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      return resp;
    }
    // This RPC is used by UserResourceQuotaManager while its singleton is being constructed.
    // Persist legacy migration before returning the recovery snapshot, but do not synchronously
    // broadcast back to DataNodes: that would re-enter getInstance() and deadlock class
    // initialization. Other DataNodes recover the same persisted map through this pull RPC.
    ensureLegacyThrottleMigrated(false);
    Map<Long, TUserResourceQuota> quotaCopy = new HashMap<>(quotaInfo.getUserResourceQuotaLimit());
    if (!quotaCopy.isEmpty()) {
      resp.setUserResourceQuota(quotaCopy);
      resp.setUserNameMap(buildUserNameMap(quotaCopy.keySet()));
    }
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }

  private Map<Long, String> buildUserNameMap(java.util.Set<Long> userIds) {
    Map<Long, String> nameMap = new HashMap<>();
    for (Long userId : userIds) {
      if (userId == null || userId < 0) {
        continue;
      }
      String name = resolveUserName(userId);
      if (name != null) {
        nameMap.put(userId, name);
      }
    }
    return nameMap;
  }

  private Map<Long, String> buildThrottleUserNameMap(java.util.Set<Long> userIds) {
    Map<Long, String> nameMap = buildUserNameMap(userIds);
    // Prefer live author names; fall back to names persisted with the throttle entry.
    for (Long userId : userIds) {
      if (userId == null || userId < 0 || nameMap.containsKey(userId)) {
        continue;
      }
      String cached = quotaInfo.getThrottleUserNames().get(userId);
      if (cached != null && !cached.isEmpty()) {
        nameMap.put(userId, cached);
      }
    }
    return nameMap;
  }

  private static Map<String, TThrottleQuota> buildLegacyThrottleQuotaMap(
      Map<Long, TThrottleQuota> throttleByUserId, Map<Long, String> userNameMap) {
    Map<String, TThrottleQuota> legacy = new HashMap<>();
    for (Map.Entry<Long, TThrottleQuota> entry : throttleByUserId.entrySet()) {
      String userName = userNameMap.get(entry.getKey());
      if (userName != null) {
        legacy.put(userName, entry.getValue());
      }
    }
    return legacy;
  }

  private String resolveUserName(long userId) {
    try {
      return configManager.getPermissionManager().getUserName(userId);
    } catch (Exception e) {
      String cached = quotaInfo.getThrottleUserNames().get(userId);
      return cached != null ? cached : String.valueOf(userId);
    }
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

  public Map<Integer, TUserResourceUsageSnapshot> getUserResourceUsageByNode() {
    return userResourceUsageByNode;
  }

  /**
   * Accept DN-side usage snapshot from dedicated report RPC (not main heartbeat), and piggyback the
   * latest user-resource quota map so DataNodes that missed SET broadcast can converge.
   */
  public TUserResourceQuotaResp reportUserResourceUsage(
      int dataNodeId, TUserResourceUsageSnapshot usage) {
    if (usage != null) {
      userResourceUsageByNode.put(dataNodeId, usage);
    } else {
      userResourceUsageByNode.put(dataNodeId, new TUserResourceUsageSnapshot());
    }
    TUserResourceQuotaResp resp = new TUserResourceQuotaResp();
    resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      return resp;
    }
    // Same as getUserResourceQuota: migrate legacy throttle keys without broadcasting (DN is
    // already up; broadcasting here is unnecessary and would amplify CN->DN traffic).
    ensureLegacyThrottleMigrated(false);
    // Always set the map (possibly empty) so DN can drop quotas deleted on CN.
    resp.setUserResourceQuota(new HashMap<>(quotaInfo.getUserResourceQuotaLimit()));
    resp.setUserNameMap(buildUserNameMap(quotaInfo.getUserResourceQuotaLimit().keySet()));
    return resp;
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
      quotaInfo.getSpaceQuotaUsage().get(entry.getKey()).setDiskSize(regionDiskCount.get());
    }
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
