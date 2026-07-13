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

package com.timecho.iotdb.manager.load;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ActivationStatusCache;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeActivateInfo;

import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;
import com.timecho.iotdb.manager.ITimechoManager;
import com.timecho.iotdb.manager.load.service.TimechoHeartbeatService;
import com.timecho.iotdb.manager.regulate.RegulateManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TimechoLoadManager extends LoadManager {

  private final ITimechoManager timechoConfigManager;

  public TimechoLoadManager(ITimechoManager configManager) {
    super(configManager);
    timechoConfigManager = configManager;
  }

  @Override
  protected void setHeartbeatService(IManager configManager, LoadCache loadCache) {
    heartbeatService = new TimechoHeartbeatService((ITimechoManager) configManager, loadCache);
  }

  /**
   * Get all Node's current activation info
   *
   * @return Map<NodeId, Node activation info
   */
  public Map<Integer, TNodeActivateInfo> getNodeSimplifiedActivateStatus() {
    return loadCache.getNodeSimplifiedActivateStatus();
  }

  public Map<Integer, String> getNodeActivateStatus() {
    Map<Integer, String> result = loadCache.getNodeActivateStatus();
    result.put(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        timechoConfigManager.getActivationManager().getActivateStatus().toString());
    return result;
  }

  /** Check if there is any active node keep living. */
  public boolean activeNodeLive() {
    return loadCache.getActivationStatusCacheMap().values().stream()
        .anyMatch(cache -> !cache.isFake() && !cache.tooOld() && cache.isActive());
  }

  /** Check if there is any active node disconnects. */
  public boolean activeNodeDisconnect() {
    return loadCache.getActivationStatusCacheMap().values().stream()
        .anyMatch(cache -> !cache.isFake() && cache.tooOld() && cache.isActive());
  }

  public boolean someConfigNodeNotSentHeartbeatYet() {
    return loadCache.getActivationStatusCacheMap().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getKey() != ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()
                    && entry.getValue().isFake());
  }

  public void updateActivationStatusCache() {
    Set<Integer> configNodeIdSet =
        configManager.getNodeManager().getRegisteredConfigNodeInfoList().stream()
            .filter(
                info ->
                    !info.status.startsWith(
                        NodeStatus.Removing.getStatus())) // Not in Removing status
            .map(TConfigNodeInfo::getConfigNodeId)
            .collect(Collectors.toSet());
    // Put if absent
    configNodeIdSet.forEach(
        id ->
            loadCache
                .getActivationStatusCacheMap()
                .compute(
                    id,
                    (key, cache) ->
                        shouldRefreshConfigNodeActivationStatusPlaceholder(cache)
                            ? createConfigNodeActivationStatusPlaceholder()
                            : cache));
    removeUnregisteredActivationStatusCaches(
        loadCache.getActivationStatusCacheMap(),
        configNodeIdSet,
        configManager.getNodeManager().getRegisteredDataNodes(),
        configManager.getNodeManager().getRegisteredAINodes());
  }

  static void removeUnregisteredActivationStatusCaches(
      Map<Integer, ActivationStatusCache> activationStatusCacheMap,
      Set<Integer> configNodeIdSet,
      Iterable<TDataNodeConfiguration> registeredDataNodes,
      Iterable<TAINodeConfiguration> registeredAINodes) {
    Set<Integer> registeredNodeIdSet = new HashSet<>(configNodeIdSet);
    registeredDataNodes.forEach(
        dataNode -> registeredNodeIdSet.add(dataNode.getLocation().getDataNodeId()));
    registeredAINodes.forEach(
        aiNode -> registeredNodeIdSet.add(aiNode.getLocation().getAiNodeId()));
    activationStatusCacheMap.keySet().removeIf(id -> !registeredNodeIdSet.contains(id));
  }

  private boolean shouldRefreshConfigNodeActivationStatusPlaceholder(
      ActivationStatusCache activationStatusCache) {
    ObligationStatus inferredStatus = inferConfigNodeActivationStatus();
    return activationStatusCache == null
        || (activationStatusCache.isFake()
            && !activationStatusCache.getActivateStatus().equals(inferredStatus));
  }

  private ActivationStatusCache createConfigNodeActivationStatusPlaceholder() {
    ObligationStatus status = inferConfigNodeActivationStatus();
    return ObligationStatus.UNKNOWN.equals(status)
        ? new ActivationStatusCache()
        : new InferredActivationStatusCache(status);
  }

  private ObligationStatus inferConfigNodeActivationStatus() {
    if (!canInferRemoteLicenseStatus()) {
      return ObligationStatus.UNKNOWN;
    }
    ObligationStatus localStatus = timechoConfigManager.getActivationManager().getActivateStatus();
    if (localStatus.isActivated()) {
      return ObligationStatus.PASSIVE_ACTIVATED;
    }
    if (localStatus.isUnactivated()) {
      return ObligationStatus.PASSIVE_UNACTIVATED;
    }
    return ObligationStatus.UNKNOWN;
  }

  private boolean canInferRemoteLicenseStatus() {
    RegulateManager activationManager = timechoConfigManager.getActivationManager();
    return activationManager.isActive()
        || activeNodeLive()
        || activationManager.activeNodeExistForLeader();
  }

  private static class InferredActivationStatusCache extends ActivationStatusCache {

    private InferredActivationStatusCache(ObligationStatus activateStatus) {
      super(System.nanoTime(), activateStatus);
    }

    @Override
    public boolean isFake() {
      return true;
    }
  }
}
