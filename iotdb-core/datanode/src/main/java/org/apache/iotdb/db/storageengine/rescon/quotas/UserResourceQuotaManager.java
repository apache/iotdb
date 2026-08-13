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

import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetUserResourceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;
import org.apache.iotdb.commons.quota.ResourceType;
import org.apache.iotdb.commons.quota.UserResourceQuota;
import org.apache.iotdb.commons.quota.UserResourceQuotaConverter;
import org.apache.iotdb.confignode.rpc.thrift.TUserResourceQuotaResp;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.conf.IoTDBConfig;
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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataNode-side user resource quota manager. Users are identified by their immutable {@code userId}
 * everywhere (quota map, node accounting, usage report); {@code userName} is tracked only to derive
 * legacy throttle limits and for logging, and follows renames.
 */
public class UserResourceQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserResourceQuotaManager.class);

  private final Map<Long, UserResourceQuota> userQuotas = new ConcurrentHashMap<>();

  /** userId -> current userName; used for legacy throttle derivation and logging only. */
  private final Map<Long, String> userNames = new ConcurrentHashMap<>();

  private final Map<ResourceType, NodeQuotaState> nodeStates = new EnumMap<>(ResourceType.class);
  private final Map<ResourceType, ResourceLimiter> limiters = new EnumMap<>(ResourceType.class);
  private final DataNodeTempDiskSpillQuotaGate tempDiskSpillQuotaGate;
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
    tempDiskSpillQuotaGate = new DataNodeTempDiskSpillQuotaGate(this);
    recover();
    startUsageReport();
    initialized = true;
  }

  public DataNodeTempDiskSpillQuotaGate getTempDiskSpillQuotaGate() {
    return tempDiskSpillQuotaGate;
  }

  private void startUsageReport() {
    ScheduledExecutorService executor =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.USER_RESOURCE_QUOTA_USAGE_REPORT.getName());
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
      TUserResourceQuotaResp resp = client.reportUserResourceUsage(nodeId, snapshotUsage());
      if (resp == null
          || resp.getStatus() == null
          || resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.debug(
            StorageEngineMessages.LOG_FAILED_TO_REPORT_USER_RESOURCE_USAGE_TO_CONFIGNODE_D08BB930
                + ": {}",
            resp == null ? null : resp.getStatus());
        return;
      }
      // Piggybacked latest quotas: heal DNs that missed SET USER QUOTA broadcast.
      if (resp.isSetUserResourceQuota()) {
        syncQuotasFromConfigNode(resp.getUserResourceQuota(), resp.getUserNameMap());
      }
    } catch (ClientManagerException | TException e) {
      LOGGER.debug(
          StorageEngineMessages.LOG_FAILED_TO_REPORT_USER_RESOURCE_USAGE_TO_CONFIGNODE_D08BB930, e);
    }
  }

  /**
   * Replace local quota map with the ConfigNode-authoritative snapshot. Unchanged users are skipped
   * to avoid resetting throttle limiters every report interval.
   */
  void syncQuotasFromConfigNode(
      Map<Long, TUserResourceQuota> remoteQuotas, Map<Long, String> userNameMap) {
    if (!isEnabled() || remoteQuotas == null) {
      return;
    }
    Map<Long, String> nameMap = userNameMap != null ? userNameMap : new HashMap<>();
    boolean changed = false;
    lock.writeLock().lock();
    try {
      // userQuotas is a ConcurrentHashMap; collect stale ids first to avoid mutating while
      // iterating
      // other quota maps under the write lock.
      List<Long> toRemove = new ArrayList<>();
      for (Long localUserId : userQuotas.keySet()) {
        if (!remoteQuotas.containsKey(localUserId)) {
          toRemove.add(localUserId);
        }
      }
      for (Long localUserId : toRemove) {
        clearUserQuotaLocked(localUserId);
        changed = true;
      }
      for (Map.Entry<Long, TUserResourceQuota> entry : remoteQuotas.entrySet()) {
        long userId = entry.getKey();
        String userName = nameMap.get(userId);
        if (UserResourceQuotaConverter.isClearRequest(entry.getValue())) {
          if (userQuotas.containsKey(userId)) {
            clearUserQuotaLocked(userId);
            changed = true;
          }
          continue;
        }
        UserResourceQuota incoming = UserResourceQuotaConverter.fromThrift(entry.getValue());
        UserResourceQuota existing = userQuotas.get(userId);
        if (Objects.equals(existing, incoming) && Objects.equals(userNames.get(userId), userName)) {
          continue;
        }
        trackUserName(userId, userName);
        userQuotas.put(userId, incoming);
        refreshNodeRanges(userId, incoming);
        syncThrottleQuota(userId, userName, incoming);
        changed = true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    if (changed) {
      LOGGER.info(
          StorageEngineMessages
              .LOG_USER_RESOURCE_QUOTAS_SYNCED_FROM_CONFIGNODE_REPORT_RESPONSE_E62A4D74);
    }
  }

  /** Clear one user's quota while the write lock is already held. */
  private void clearUserQuotaLocked(long userId) {
    userQuotas.remove(userId);
    for (NodeQuotaState node : nodeStates.values()) {
      node.clearUserRange(userId);
    }
    userNames.remove(userId);
    DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().removeQuota(userId);
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

  /**
   * Apply latest {@code dn_quota_*} capacities from {@link IoTDBConfig} into live node states. Safe
   * to call from hot-reload; no-op when this singleton has not been constructed yet.
   */
  public void reloadNodeCapacitiesFromConfig() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long cpuCapacity = config.getDnQuotaCpuSlots();
    long memCapacity = config.getDnQuotaMemoryBytes();
    long tempDiskCapacity = config.getDnQuotaTempDiskBytes();
    lock.writeLock().lock();
    try {
      NodeQuotaState cpu = nodeStates.get(ResourceType.CPU);
      if (cpu != null) {
        cpu.setNodeCapacity(cpuCapacity);
      }
      NodeQuotaState mem = nodeStates.get(ResourceType.MEMORY);
      if (mem != null) {
        mem.setNodeCapacity(memCapacity);
      }
      NodeQuotaState tempDisk = nodeStates.get(ResourceType.TEMP_DISK);
      if (tempDisk != null) {
        tempDisk.setNodeCapacity(tempDiskCapacity);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Try-acquire one resource under write-lock, retrying until the policy deadline. Unconfigured /
   * unlimited users still compete for node capacity so configured min guarantees are not starved.
   */
  public AcquireResult acquire(
      long userId,
      OperationType op,
      ResourceType resource,
      long amount,
      AcquireContext ctx,
      AcquirePolicy policy) {
    if (!isEnabled() || isExempt(userId)) {
      return AcquireResult.success(new QuotaToken(this, userId, op, resource, 0));
    }
    // Unconfigured / unlimited users still go through capacity scheduling so that
    // configured min guarantees are not starved (unlimited != skip node checks).
    ResourceQuotaRange range = getRange(userId, op, resource);
    ResourceLimiter limiter = limiters.get(resource);
    if (limiter == null || !limiter.isEnforced()) {
      return AcquireResult.success(new QuotaToken(this, userId, op, resource, 0));
    }
    NodeQuotaState node = nodeStates.get(resource);
    if (node == null) {
      return AcquireResult.success(new QuotaToken(this, userId, op, resource, 0));
    }

    long deadline = System.currentTimeMillis() + policy.getMaxWaitMs();
    String lastRejectReason = StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_89601D9A;
    while (true) {
      lock.writeLock().lock();
      try {
        LimiterAcquireResult attempt = limiter.tryAcquire(userId, op, amount, range, node);
        if (attempt.isSuccess()) {
          return AcquireResult.success(new QuotaToken(this, userId, op, resource, amount));
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
        logAcquireRejected(userId, op, resource, reason);
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
        logAcquireRejected(userId, op, resource, reason);
        return AcquireResult.reject(reason);
      }
    }
  }

  private void logAcquireRejected(
      long userId, OperationType op, ResourceType resource, String reason) {
    LOGGER.warn(
        StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_ACQUIRE_REJECTED_F5079ADB,
        displayName(userId),
        op.name().toLowerCase(),
        resource.name().toLowerCase(),
        reason);
    try {
      if (CommonDescriptor.getInstance().getConfig().isEnableAuditLog()) {
        AuditLogFields fields =
            new AuditLogFields(
                userId,
                displayName(userId),
                null,
                AuditEventType.USER_RESOURCE_QUOTA_REJECTED,
                AuditLogOperation.CONTROL,
                (PrivilegeType) null,
                false,
                null,
                String.format("%s %s: %s", op.name().toLowerCase(), resource.name(), reason));
        DNAuditLogger.getInstance().log(fields, () -> reason, System::currentTimeMillis);
      }
    } catch (Exception e) {
      LOGGER.debug(
          StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_ACQUIRE_REJECTED_F5079ADB,
          displayName(userId),
          op.name().toLowerCase(),
          resource.name().toLowerCase(),
          e.getMessage());
    }
  }

  private String displayName(long userId) {
    String name = userNames.get(userId);
    return name == null ? String.valueOf(userId) : name;
  }

  public QuotaToken acquireOrThrow(
      long userId,
      OperationType op,
      ResourceType resource,
      long amount,
      AcquireContext ctx,
      AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    AcquireResult result = acquire(userId, op, resource, amount, ctx, policy);
    if (!result.isSuccess()) {
      throw new UserResourceQuotaExceededException(result.getRejectReason(), resource);
    }
    return result.getToken();
  }

  /** Amount-based release counterpart for token-less accounting (e.g. spill TEMP_DISK). */
  public void releaseAmount(long userId, OperationType op, ResourceType resource, long amount) {
    if (!isEnabled() || amount <= 0) {
      return;
    }
    ResourceLimiter limiter = limiters.get(resource);
    NodeQuotaState node = nodeStates.get(resource);
    if (limiter == null || node == null) {
      return;
    }
    lock.writeLock().lock();
    try {
      limiter.release(userId, op, amount, node);
    } finally {
      lock.writeLock().unlock();
    }
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
      limiter.release(token.getUserId(), token.getOp(), token.getAmount(), node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getInUse(long userId, OperationType op, ResourceType resource) {
    NodeQuotaState node = nodeStates.get(resource);
    if (node == null) {
      return 0;
    }
    return node.inUse(op, userId);
  }

  public void updateQuota(long userId, String userName, UserResourceQuota quota) {
    updateQuotaInternal(userId, userName, quota, true);
  }

  void updateQuotaWithoutThrottleSync(long userId, String userName, UserResourceQuota quota) {
    updateQuotaInternal(userId, userName, quota, false);
  }

  private void updateQuotaInternal(
      long userId, String userName, UserResourceQuota quota, boolean syncThrottle) {
    if (quota == null) {
      return;
    }
    lock.writeLock().lock();
    try {
      trackUserName(userId, userName);
      UserResourceQuota existing = userQuotas.computeIfAbsent(userId, k -> new UserResourceQuota());
      existing.mergeFrom(quota);
      refreshNodeRanges(userId, existing);
      if (syncThrottle) {
        syncThrottleQuota(userId, userName, existing);
      }
    } finally {
      lock.writeLock().unlock();
    }
    LOGGER.info(
        StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_UPDATED_3C8F5A7B, displayName(userId));
  }

  /** Track userId -> userName for display / logging only. */
  private void trackUserName(long userId, String userName) {
    if (userName == null) {
      return;
    }
    userNames.put(userId, userName);
  }

  public void setUserResourceQuota(TSetUserResourceQuotaReq req) {
    if (!EditionGate.isUserResourceQuotaEnabled()) {
      return;
    }
    if (!req.isSetUserId()) {
      // Defensive: CN -> DN broadcasts always carry userId; ignore malformed requests.
      LOGGER.warn(
          StorageEngineMessages.LOG_IGNORE_USER_RESOURCE_QUOTA_BROADCAST_WITHOUT_USERID_B32A7B22,
          req.getUserName());
      return;
    }
    if (UserResourceQuotaConverter.isClearRequest(req.getUserResourceQuota())) {
      clearUserQuota(req.getUserId(), req.getUserName());
      return;
    }
    updateQuota(
        req.getUserId(),
        req.getUserName(),
        UserResourceQuotaConverter.fromThrift(req.getUserResourceQuota()));
  }

  /** True when CN broadcasts an empty quota meaning DELETE USER QUOTA. */
  static boolean isClearRequest(TUserResourceQuota quota) {
    return UserResourceQuotaConverter.isClearRequest(quota);
  }

  public void clearUserQuota(long userId, String userName) {
    lock.writeLock().lock();
    try {
      clearUserQuotaLocked(userId);
    } finally {
      lock.writeLock().unlock();
    }
    LOGGER.info(
        StorageEngineMessages.LOG_USER_RESOURCE_QUOTA_UPDATED_3C8F5A7B,
        userName == null ? String.valueOf(userId) : userName);
  }

  public long getMinGap(long userId, OperationType op, ResourceType resource) {
    ResourceQuotaRange range = getRange(userId, op, resource);
    if (range == null
        || range.getMinValue() == IoTDBConstant.UNLIMITED_VALUE
        || range.getMinValue() < 0) {
      return 0;
    }
    long inUse = getInUse(userId, op, resource);
    return Math.max(0, range.getMinValue() - inUse);
  }

  public UserResourceQuota getUserQuota(long userId) {
    return userQuotas.get(userId);
  }

  public Map<Long, UserResourceQuota> getAllUserQuotas() {
    return new HashMap<>(userQuotas);
  }

  /** Snapshot current in-use for dedicated report RPC to ConfigNode (SHOW aggregation). */
  public TUserResourceUsageSnapshot snapshotUsage() {
    TUserResourceUsageSnapshot snap = new TUserResourceUsageSnapshot();
    Map<Long, Map<TResourceType, Long>> read = new HashMap<>();
    Map<Long, Map<TResourceType, Long>> write = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<ResourceType, NodeQuotaState> entry : nodeStates.entrySet()) {
        TResourceType tType = toThriftType(entry.getKey());
        if (tType == null) {
          continue;
        }
        fillUsageSide(read, entry.getValue(), OperationType.READ, tType);
        fillUsageSide(write, entry.getValue(), OperationType.WRITE, tType);
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

  private static void fillUsageSide(
      Map<Long, Map<TResourceType, Long>> target,
      NodeQuotaState node,
      OperationType op,
      TResourceType tType) {
    for (Map.Entry<Long, Long> usage : node.getInUseByUser(op).entrySet()) {
      if (usage.getValue() == null || usage.getValue() <= 0) {
        continue;
      }
      target.computeIfAbsent(usage.getKey(), k -> new HashMap<>()).put(tType, usage.getValue());
    }
  }

  private static TResourceType toThriftType(ResourceType resource) {
    return switch (resource) {
      case CPU -> TResourceType.CPU;
      case MEMORY -> TResourceType.MEMORY;
      case TEMP_DISK -> TResourceType.TEMP_DISK;
      case DISK_IO -> TResourceType.DISK_IO;
      default -> null;
    };
  }

  public int getNodeId() {
    return nodeId;
  }

  public NodeQuotaState getNodeState(ResourceType resource) {
    return nodeStates.get(resource);
  }

  private ResourceQuotaRange getRange(long userId, OperationType op, ResourceType resource) {
    UserResourceQuota quota = userQuotas.get(userId);
    if (quota == null) {
      return null;
    }
    return quota.getRange(op, resource);
  }

  private void refreshNodeRanges(long userId, UserResourceQuota quota) {
    for (OperationType op : OperationType.values()) {
      Map<ResourceType, ResourceQuotaRange> ranges =
          op == OperationType.READ ? quota.getReadQuota() : quota.getWriteQuota();
      for (ResourceType resource : nodeStates.keySet()) {
        NodeQuotaState node = nodeStates.get(resource);
        if (node == null) {
          continue;
        }
        // Explicitly clear stale side when attribute removed from a later SET/merge.
        node.updateRange(op, userId, ranges.get(resource));
      }
    }
  }

  private void syncThrottleQuota(long userId, String userName, UserResourceQuota quota) {
    TThrottleQuota throttle = UserResourceQuotaConverter.toThrottleQuota(quota);
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserId(userId);
    req.setUserName(userName == null ? String.valueOf(userId) : userName);
    req.setThrottleQuota(throttle);
    DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit().setQuotas(req);
  }

  private void recover() {
    TUserResourceQuotaResp resp = ClusterConfigTaskExecutor.getInstance().getUserResourceQuota();
    if (resp == null || resp.getUserResourceQuota() == null) {
      return;
    }
    Map<Long, String> nameMap =
        resp.isSetUserNameMap() && resp.getUserNameMap() != null
            ? resp.getUserNameMap()
            : new HashMap<>();
    for (Map.Entry<Long, TUserResourceQuota> entry : resp.getUserResourceQuota().entrySet()) {
      // Do not sync throttle during recover; DataNodeThrottleQuotaManager recovers separately.
      updateQuotaWithoutThrottleSync(
          entry.getKey(),
          nameMap.get(entry.getKey()),
          UserResourceQuotaConverter.fromThrift(entry.getValue()));
    }
  }

  public QuotaTokenBundle acquireReadResources(
      long userId, long memoryBytes, AcquireContext ctx, AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    return acquireResources(userId, OperationType.READ, memoryBytes, ctx, policy);
  }

  public QuotaTokenBundle acquireWriteResources(
      long userId, long memoryBytes, AcquireContext ctx, AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    return acquireResources(userId, OperationType.WRITE, memoryBytes, ctx, policy);
  }

  /** Acquire CPU then MEMORY; TEMP_DISK is charged later only on actual spill. */
  private QuotaTokenBundle acquireResources(
      long userId, OperationType op, long memoryBytes, AcquireContext ctx, AcquirePolicy policy)
      throws UserResourceQuotaExceededException {
    AcquireResult cpu = acquire(userId, op, ResourceType.CPU, 1, ctx, policy);
    if (!cpu.isSuccess()) {
      throw new UserResourceQuotaExceededException(cpu.getRejectReason(), ResourceType.CPU);
    }
    AcquireResult mem = acquire(userId, op, ResourceType.MEMORY, memoryBytes, ctx, policy);
    if (!mem.isSuccess()) {
      release(cpu.getToken());
      throw new UserResourceQuotaExceededException(mem.getRejectReason(), ResourceType.MEMORY);
    }
    // TEMP_DISK is intentionally NOT acquired here: it is charged only when the query actually
    // spills to disk (external sort), see TempDiskSpillQuotaGate.
    return new QuotaTokenBundle(cpu.getToken(), mem.getToken());
  }

  private boolean isEnabled() {
    return EditionGate.isUserResourceQuotaEnabled()
        && IoTDBDescriptor.getInstance().getConfig().isQuotaEnable();
  }

  private boolean isExempt(long userId) {
    return userId == IoTDBConstant.SUPER_USER_ID;
  }
}
