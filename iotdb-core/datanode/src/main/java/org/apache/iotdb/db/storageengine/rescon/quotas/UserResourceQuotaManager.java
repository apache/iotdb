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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetUserResourceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;
import org.apache.iotdb.commons.quota.ResourceType;
import org.apache.iotdb.commons.quota.UserResourceQuota;
import org.apache.iotdb.commons.quota.UserResourceQuotaConverter;
import org.apache.iotdb.confignode.rpc.thrift.TUserResourceQuotaResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserResourceQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserResourceQuotaManager.class);

  private final Map<String, UserResourceQuota> userQuotas = new ConcurrentHashMap<>();
  private final Map<ResourceType, NodeQuotaState> nodeStates = new EnumMap<>(ResourceType.class);
  private final Map<ResourceType, ResourceLimiter> limiters = new EnumMap<>(ResourceType.class);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final int nodeId;
  private static volatile boolean initialized = false;

  /** Low-frequency dedicated report; do not piggyback on main DataNode heartbeat. */
  private static final long USAGE_REPORT_INTERVAL_SECONDS = 10L;

  private UserResourceQuotaManager() {
    nodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    long cpuCapacity = IoTDBDescriptor.getInstance().getConfig().getDnQuotaCpuSlots();
    long memCapacity = IoTDBDescriptor.getInstance().getConfig().getDnQuotaMemoryBytes();
    long tempDiskCapacity = IoTDBDescriptor.getInstance().getConfig().getDnQuotaTempDiskBytes();
    nodeStates.put(ResourceType.CPU, new NodeQuotaState(nodeId, cpuCapacity));
    nodeStates.put(ResourceType.MEMORY, new NodeQuotaState(nodeId, memCapacity));
    nodeStates.put(ResourceType.TEMP_DISK, new NodeQuotaState(nodeId, tempDiskCapacity));
    limiters.put(ResourceType.CPU, new CpuSlotLimiter());
    limiters.put(ResourceType.MEMORY, new MemoryBudgetLimiter());
    limiters.put(ResourceType.DISK_IO, new DiskIoLimiter());
    limiters.put(ResourceType.TEMP_DISK, new TempDiskLimiter());
    recover();
    startUsageReport();
    initialized = true;
  }

  private void startUsageReport() {
    ScheduledExecutorService executor =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("UserResourceQuota-UsageReport");
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        executor,
        this::reportUsageToConfigNode,
        USAGE_REPORT_INTERVAL_SECONDS,
        USAGE_REPORT_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
    LOGGER.info(
        String.format(
            StorageEngineMessages
                .LOG_USER_RESOURCE_USAGE_REPORT_STARTED_WITH_INTERVAL_ARG_SECONDS_C3CC4CC2,
            USAGE_REPORT_INTERVAL_SECONDS));
  }

  private void reportUsageToConfigNode() {
    if (!isEnabled()) {
      return;
    }
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus status = client.reportUserResourceUsage(nodeId, snapshotUsage());
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.debug(
            StorageEngineMessages.LOG_FAILED_TO_REPORT_USER_RESOURCE_USAGE_TO_CONFIGNODE_D08BB930
                + ": {}",
            status);
      }
    } catch (ClientManagerException | TException e) {
      LOGGER.debug(
          StorageEngineMessages.LOG_FAILED_TO_REPORT_USER_RESOURCE_USAGE_TO_CONFIGNODE_D08BB930, e);
    }
  }

  private static class Holder {
    private static final UserResourceQuotaManager INSTANCE = new UserResourceQuotaManager();
  }

  public static UserResourceQuotaManager getInstance() {
    return Holder.INSTANCE;
  }

  public static boolean isInitialized() {
    return initialized;
  }

  public AcquireResult acquire(
      String user,
      OperationType op,
      ResourceType resource,
      long amount,
      AcquireContext ctx,
      AcquirePolicy policy) {
    if (!isEnabled() || isExempt(user)) {
      return AcquireResult.success(new QuotaToken(this, user, op, resource, 0));
    }
    // Unconfigured / unlimited users still go through capacity scheduling so that
    // configured min guarantees are not starved (unlimited != skip node checks).
    ResourceQuotaRange range = getRange(user, op, resource);
    ResourceLimiter limiter = limiters.get(resource);
    if (limiter == null || !limiter.isEnforced()) {
      return AcquireResult.success(new QuotaToken(this, user, op, resource, 0));
    }
    NodeQuotaState node = nodeStates.get(resource);
    if (node == null) {
      return AcquireResult.success(new QuotaToken(this, user, op, resource, 0));
    }

    long deadline = System.currentTimeMillis() + policy.getMaxWaitMs();
    String lastRejectReason = StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_89601D9A;
    while (true) {
      lock.writeLock().lock();
      try {
        LimiterAcquireResult attempt = limiter.tryAcquire(user, amount, range, node);
        if (attempt.isSuccess()) {
          return AcquireResult.success(new QuotaToken(this, user, op, resource, amount));
        }
        lastRejectReason = attempt.getRejectReason();
      } finally {
        lock.writeLock().unlock();
      }
      if (System.currentTimeMillis() >= deadline) {
        String reason =
            String.format(
                    StorageEngineMessages.EXCEPTION_USER_RESOURCE_QUOTA_WAIT_TIMEOUT_6F3A1C2D,
                    op.name().toLowerCase(),
                    resource.name().toLowerCase())
                + ": "
                + lastRejectReason;
        logAcquireRejected(user, op, resource, reason);
        return AcquireResult.reject(reason);
      }
      try {
        Thread.sleep(policy.getRetryIntervalMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        String reason =
            String.format(
                StorageEngineMessages.EXCEPTION_USER_RESOURCE_QUOTA_ACQUIRE_INTERRUPTED_31D4116D,
                op.name().toLowerCase() + " " + resource.name().toLowerCase());
        logAcquireRejected(user, op, resource, reason);
        return AcquireResult.reject(reason);
      }
    }
  }

  private void logAcquireRejected(
      String user, OperationType op, ResourceType resource, String reason) {
    LOGGER.warn(
        StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_ACQUIRE_REJECTED_F5079ADB,
        user,
        op.name().toLowerCase(),
        resource.name().toLowerCase(),
        reason);
  }

  public QuotaToken acquireOrThrow(
      String user,
      OperationType op,
      ResourceType resource,
      long amount,
      AcquireContext ctx,
      AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    AcquireResult result = acquire(user, op, resource, amount, ctx, policy);
    if (!result.isSuccess()) {
      throw new UserResourceQuotaExceededException(result.getRejectReason());
    }
    return result.getToken();
  }

  public void release(QuotaToken token) {
    if (!isEnabled() || token.getAmount() <= 0) {
      return;
    }
    ResourceLimiter limiter = limiters.get(token.getResource());
    NodeQuotaState node = nodeStates.get(token.getResource());
    if (limiter == null || node == null) {
      return;
    }
    lock.writeLock().lock();
    try {
      limiter.release(token.getUser(), token.getAmount(), node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getInUse(String user, OperationType op, ResourceType resource) {
    NodeQuotaState node = nodeStates.get(resource);
    if (node == null) {
      return 0;
    }
    return node.inUse(user);
  }

  public void updateQuota(String user, UserResourceQuota quota) {
    updateQuotaInternal(user, quota, true);
  }

  void updateQuotaWithoutThrottleSync(String user, UserResourceQuota quota) {
    updateQuotaInternal(user, quota, false);
  }

  private void updateQuotaInternal(String user, UserResourceQuota quota, boolean syncThrottle) {
    if (quota == null) {
      return;
    }
    lock.writeLock().lock();
    try {
      UserResourceQuota existing = userQuotas.computeIfAbsent(user, k -> new UserResourceQuota());
      existing.mergeFrom(quota);
      refreshNodeRanges(user, existing);
      if (syncThrottle) {
        syncThrottleQuota(user, existing);
      }
    } finally {
      lock.writeLock().unlock();
    }
    LOGGER.info(StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_UPDATED_3C8F5A7B, user);
  }

  public void setUserResourceQuota(TSetUserResourceQuotaReq req) {
    if (isClearRequest(req.getUserResourceQuota())) {
      clearUserQuota(req.getUserName());
      return;
    }
    updateQuota(
        req.getUserName(), UserResourceQuotaConverter.fromThrift(req.getUserResourceQuota()));
  }

  /** True when CN broadcasts an empty quota meaning DELETE USER QUOTA. */
  static boolean isClearRequest(TUserResourceQuota quota) {
    if (quota == null) {
      return true;
    }
    boolean readEmpty =
        !quota.isSetReadQuota() || quota.getReadQuota() == null || quota.getReadQuota().isEmpty();
    boolean writeEmpty =
        !quota.isSetWriteQuota()
            || quota.getWriteQuota() == null
            || quota.getWriteQuota().isEmpty();
    boolean throttleEmpty =
        !quota.isSetThrottleLimit()
            || quota.getThrottleLimit() == null
            || quota.getThrottleLimit().isEmpty();
    return readEmpty && writeEmpty && throttleEmpty;
  }

  public void clearUserQuota(String user) {
    lock.writeLock().lock();
    try {
      userQuotas.remove(user);
      for (NodeQuotaState node : nodeStates.values()) {
        // Keep inUse for in-flight tokens; only clear configured ranges (DELETE = unlimited).
        node.clearUserRange(user);
      }
      DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().removeQuota(user);
    } finally {
      lock.writeLock().unlock();
    }
    LOGGER.info(StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_UPDATED_3C8F5A7B, user);
  }

  public long getMinGap(String user, OperationType op, ResourceType resource) {
    ResourceQuotaRange range = getRange(user, op, resource);
    if (range == null
        || range.getMinValue() == IoTDBConstant.UNLIMITED_VALUE
        || range.getMinValue() < 0) {
      return 0;
    }
    long inUse = getInUse(user, op, resource);
    return Math.max(0, range.getMinValue() - inUse);
  }

  public UserResourceQuota getUserQuota(String user) {
    return userQuotas.get(user);
  }

  public Map<String, UserResourceQuota> getAllUserQuotas() {
    return new HashMap<>(userQuotas);
  }

  /** Snapshot current in-use for dedicated report RPC to ConfigNode (SHOW aggregation). */
  public org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot snapshotUsage() {
    org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot snap =
        new org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot();
    Map<String, Map<org.apache.iotdb.common.rpc.thrift.TResourceType, Long>> read = new HashMap<>();
    Map<String, Map<org.apache.iotdb.common.rpc.thrift.TResourceType, Long>> write =
        new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<ResourceType, NodeQuotaState> entry : nodeStates.entrySet()) {
        org.apache.iotdb.common.rpc.thrift.TResourceType tType = toThriftType(entry.getKey());
        if (tType == null) {
          continue;
        }
        for (Map.Entry<String, Long> usage : entry.getValue().getInUseByUser().entrySet()) {
          if (usage.getValue() == null || usage.getValue() <= 0) {
            continue;
          }
          read.computeIfAbsent(usage.getKey(), k -> new HashMap<>()).put(tType, usage.getValue());
          write.computeIfAbsent(usage.getKey(), k -> new HashMap<>()).put(tType, usage.getValue());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    if (!read.isEmpty()) {
      snap.setReadInUse(read);
    }
    if (!write.isEmpty()) {
      snap.setWriteInUse(write);
    }
    return snap;
  }

  private static org.apache.iotdb.common.rpc.thrift.TResourceType toThriftType(
      ResourceType resource) {
    switch (resource) {
      case CPU:
        return org.apache.iotdb.common.rpc.thrift.TResourceType.CPU;
      case MEMORY:
        return org.apache.iotdb.common.rpc.thrift.TResourceType.MEMORY;
      case TEMP_DISK:
        return org.apache.iotdb.common.rpc.thrift.TResourceType.TEMP_DISK;
      case DISK_IO:
        return org.apache.iotdb.common.rpc.thrift.TResourceType.DISK_IO;
      default:
        return null;
    }
  }

  public int getNodeId() {
    return nodeId;
  }

  public NodeQuotaState getNodeState(ResourceType resource) {
    return nodeStates.get(resource);
  }

  private ResourceQuotaRange getRange(String user, OperationType op, ResourceType resource) {
    UserResourceQuota quota = userQuotas.get(user);
    if (quota == null) {
      return null;
    }
    return quota.getRange(op, resource);
  }

  private void refreshNodeRanges(String user, UserResourceQuota quota) {
    for (OperationType op : OperationType.values()) {
      Map<ResourceType, ResourceQuotaRange> ranges =
          op == OperationType.READ ? quota.getReadQuota() : quota.getWriteQuota();
      for (Map.Entry<ResourceType, ResourceQuotaRange> entry : ranges.entrySet()) {
        NodeQuotaState node = nodeStates.get(entry.getKey());
        if (node != null) {
          node.updateRange(user, entry.getValue());
        }
      }
    }
  }

  private void syncThrottleQuota(String user, UserResourceQuota quota) {
    TThrottleQuota throttle = UserResourceQuotaConverter.toThrottleQuota(quota);
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserName(user);
    req.setThrottleQuota(throttle);
    DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().setQuotas(req);
  }

  private void recover() {
    TUserResourceQuotaResp resp = ClusterConfigTaskExecutor.getInstance().getUserResourceQuota();
    if (resp == null || resp.getUserResourceQuota() == null) {
      return;
    }
    for (Map.Entry<String, TUserResourceQuota> entry : resp.getUserResourceQuota().entrySet()) {
      // Do not sync throttle during recover; DataNodeThrottleQuotaManager recovers separately.
      updateQuotaWithoutThrottleSync(
          entry.getKey(), UserResourceQuotaConverter.fromThrift(entry.getValue()));
    }
  }

  public QuotaTokenBundle acquireReadResources(
      String user, long memoryBytes, AcquireContext ctx, AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    AcquireResult cpu = acquire(user, OperationType.READ, ResourceType.CPU, 1, ctx, policy);
    if (!cpu.isSuccess()) {
      throw new UserResourceQuotaExceededException(cpu.getRejectReason());
    }
    AcquireResult mem =
        acquire(user, OperationType.READ, ResourceType.MEMORY, memoryBytes, ctx, policy);
    if (!mem.isSuccess()) {
      release(cpu.getToken());
      throw new UserResourceQuotaExceededException(mem.getRejectReason());
    }
    AcquireResult tempDisk =
        acquire(user, OperationType.READ, ResourceType.TEMP_DISK, memoryBytes, ctx, policy);
    if (!tempDisk.isSuccess()) {
      release(mem.getToken());
      release(cpu.getToken());
      throw new UserResourceQuotaExceededException(tempDisk.getRejectReason());
    }
    return new QuotaTokenBundle(cpu.getToken(), mem.getToken(), tempDisk.getToken());
  }

  public QuotaTokenBundle acquireWriteResources(
      String user, long memoryBytes, AcquireContext ctx, AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    AcquireResult cpu = acquire(user, OperationType.WRITE, ResourceType.CPU, 1, ctx, policy);
    if (!cpu.isSuccess()) {
      throw new UserResourceQuotaExceededException(cpu.getRejectReason());
    }
    AcquireResult mem =
        acquire(user, OperationType.WRITE, ResourceType.MEMORY, memoryBytes, ctx, policy);
    if (!mem.isSuccess()) {
      release(cpu.getToken());
      throw new UserResourceQuotaExceededException(mem.getRejectReason());
    }
    AcquireResult tempDisk =
        acquire(user, OperationType.WRITE, ResourceType.TEMP_DISK, memoryBytes, ctx, policy);
    if (!tempDisk.isSuccess()) {
      release(mem.getToken());
      release(cpu.getToken());
      throw new UserResourceQuotaExceededException(tempDisk.getRejectReason());
    }
    return new QuotaTokenBundle(cpu.getToken(), mem.getToken(), tempDisk.getToken());
  }

  private boolean isEnabled() {
    return IoTDBDescriptor.getInstance().getConfig().isQuotaEnable();
  }

  private boolean isExempt(String user) {
    return IoTDBConstant.PATH_ROOT.equals(user);
  }
}
