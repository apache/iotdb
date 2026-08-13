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

package org.apache.iotdb.confignode.persistence.quota;

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.UserResourceQuotaConverter;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.quota.DeleteUserResourceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetUserResourceQuotaPlan;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToLongFunction;

public class QuotaInfo implements SnapshotProcessor {

  private static final Logger logger = LoggerFactory.getLogger(QuotaInfo.class);

  /**
   * Marker written before the throttle-quota map so loaders can distinguish the userId-keyed format
   * from the legacy userName-keyed format (where the first int was the map size, always &gt;= 0).
   */
  private static final int THROTTLE_QUOTA_USER_ID_FORMAT = -1;

  private final ReentrantReadWriteLock spaceQuotaReadWriteLock;
  private final Map<String, TSpaceQuota> spaceQuotaLimit;
  private final Map<String, TSpaceQuota> spaceQuotaUsage;

  /** userId (immutable, rename-safe) -> throttle quota. */
  private final Map<Long, TThrottleQuota> throttleQuotaLimit;

  /** userId -> last known userName (for SHOW / DN recover display). */
  private final Map<Long, String> throttleUserNames;

  /**
   * Legacy snapshot / raft entries that still keyed throttle by userName. Drained by {@link
   * #upgradeLegacyThrottleKeys(ToLongFunction)} once AuthorInfo can resolve userIds.
   */
  private final Map<String, TThrottleQuota> legacyThrottleByUserName;

  /** userId (immutable, rename-safe) -> user resource quota. */
  private final Map<Long, TUserResourceQuota> userResourceQuotaLimit;

  /**
   * userIds whose throttle quota has not yet been mirrored into the userId-keyed user resource
   * quota map (TimechoDB only). In-memory only; re-populated after snapshot load when needed.
   */
  private final Set<Long> pendingThrottleMigration;

  /**
   * Set when a legacy snapshot had no user-resource section; after throttle keys are upgraded, all
   * throttle userIds are added to {@link #pendingThrottleMigration}.
   */
  private boolean mirrorAllThrottleAfterUpgrade;

  private final String snapshotFileName = "quota_info.bin";

  public QuotaInfo() {
    spaceQuotaReadWriteLock = new ReentrantReadWriteLock();
    spaceQuotaLimit = new HashMap<>();
    spaceQuotaUsage = new HashMap<>();
    throttleQuotaLimit = new HashMap<>();
    throttleUserNames = new HashMap<>();
    legacyThrottleByUserName = new HashMap<>();
    userResourceQuotaLimit = new HashMap<>();
    pendingThrottleMigration = new HashSet<>();
  }

  public TSStatus setSpaceQuota(SetSpaceQuotaPlan setSpaceQuotaPlan) {
    for (String database : setSpaceQuotaPlan.getPrefixPathList()) {
      TSpaceQuota spaceQuota = setSpaceQuotaPlan.getSpaceLimit();
      // “DEFAULT_VALUE” means that the user has not reset the value of the space quota type
      // So the old values are still used
      if (spaceQuotaLimit.containsKey(database)) {
        if (spaceQuota.getDeviceNum() == IoTDBConstant.DEFAULT_VALUE) {
          spaceQuota.setDeviceNum(spaceQuotaLimit.get(database).getDeviceNum());
        }
        if (spaceQuota.getTimeserieNum() == IoTDBConstant.DEFAULT_VALUE) {
          spaceQuota.setTimeserieNum(spaceQuotaLimit.get(database).getTimeserieNum());
        }
        if (spaceQuota.getDiskSize() == IoTDBConstant.DEFAULT_VALUE) {
          spaceQuota.setDiskSize(spaceQuotaLimit.get(database).getDiskSize());
        }
        if (spaceQuota.getDeviceNum() == IoTDBConstant.UNLIMITED_VALUE) {
          spaceQuota.setDeviceNum(IoTDBConstant.DEFAULT_VALUE);
        }
        if (spaceQuota.getTimeserieNum() == IoTDBConstant.UNLIMITED_VALUE) {
          spaceQuota.setTimeserieNum(IoTDBConstant.DEFAULT_VALUE);
        }
        if (spaceQuota.getDiskSize() == IoTDBConstant.UNLIMITED_VALUE) {
          spaceQuota.setDiskSize(IoTDBConstant.DEFAULT_VALUE);
        }
      }
      spaceQuotaUsage.computeIfAbsent(database, k -> new TSpaceQuota());
      spaceQuotaLimit.put(database, spaceQuota);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * Merge throttle by userId (legacy name-keyed fallback); mark pending USER QUOTA migration when
   * the feature is enabled.
   */
  public TSStatus setThrottleQuota(SetThrottleQuotaPlan setThrottleQuotaPlan) {
    TThrottleQuota throttleQuota = setThrottleQuotaPlan.getThrottleQuota();
    long userId = setThrottleQuotaPlan.getUserId();
    String userName = setThrottleQuotaPlan.getUserName();
    if (userId < 0) {
      // Legacy raft entry without userId and AuthorInfo could not resolve it yet.
      if (userName != null) {
        legacyThrottleByUserName.put(userName, throttleQuota);
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
    if (userName != null) {
      throttleUserNames.put(userId, userName);
      legacyThrottleByUserName.remove(userName);
    }
    if (throttleQuotaLimit.containsKey(userId)) {
      TThrottleQuota existing = throttleQuotaLimit.get(userId);
      // about memory
      if (setThrottleQuotaPlan.getThrottleQuota().getMemLimit() == IoTDBConstant.UNLIMITED_VALUE) {
        existing.setMemLimit(IoTDBConstant.DEFAULT_VALUE);
      } else if (setThrottleQuotaPlan.getThrottleQuota().getMemLimit()
          != IoTDBConstant.DEFAULT_VALUE) {
        existing.setMemLimit(throttleQuota.getMemLimit());
      }

      // about cpu
      if (setThrottleQuotaPlan.getThrottleQuota().getCpuLimit() == IoTDBConstant.UNLIMITED_VALUE) {
        existing.setCpuLimit(IoTDBConstant.DEFAULT_VALUE);
      } else if (setThrottleQuotaPlan.getThrottleQuota().getCpuLimit()
          != IoTDBConstant.DEFAULT_VALUE) {
        existing.setCpuLimit(throttleQuota.getCpuLimit());
      }
      if (!throttleQuota.getThrottleLimit().isEmpty()) {
        for (ThrottleType throttleType : throttleQuota.getThrottleLimit().keySet()) {
          if (existing.getThrottleLimit().containsKey(throttleType)) {
            existing
                .getThrottleLimit()
                .get(throttleType)
                .setSoftLimit(throttleQuota.getThrottleLimit().get(throttleType).getSoftLimit());
            existing
                .getThrottleLimit()
                .get(throttleType)
                .setTimeUnit(throttleQuota.getThrottleLimit().get(throttleType).getTimeUnit());
          } else {
            existing
                .getThrottleLimit()
                .put(throttleType, throttleQuota.getThrottleLimit().get(throttleType));
          }
        }
      }
    } else {
      throttleQuotaLimit.put(userId, setThrottleQuotaPlan.getThrottleQuota());
    }
    // TimechoDB: mirror into the userId-keyed user resource quota map on the leader.
    if (org.apache.iotdb.commons.conf.EditionGate.isUserResourceQuotaEnabled()) {
      pendingThrottleMigration.add(userId);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * Merge partial USER QUOTA into the persisted map (seed from throttle if absent), validate
   * ranges, and sync derived throttle fields.
   */
  public TSStatus setUserResourceQuota(SetUserResourceQuotaPlan plan) {
    long userId = plan.getUserId();
    String userName = plan.getUserName();
    TUserResourceQuota incoming = plan.getUserResourceQuota();
    // Seed from throttle so partial USER QUOTA updates do not wipe cpu/mem limits.
    if (!userResourceQuotaLimit.containsKey(userId) && throttleQuotaLimit.containsKey(userId)) {
      userResourceQuotaLimit.put(
          userId,
          UserResourceQuotaConverter.toThrift(
              UserResourceQuotaConverter.fromThrottleQuota(throttleQuotaLimit.get(userId))));
    }
    TUserResourceQuota merged =
        userResourceQuotaLimit.containsKey(userId)
            ? UserResourceQuotaConverter.toThrift(
                UserResourceQuotaConverter.fromThrift(userResourceQuotaLimit.get(userId)))
            : new TUserResourceQuota();
    mergeUserResourceQuota(merged, incoming);
    TSStatus validationStatus = validateUserResourceQuota(merged);
    if (validationStatus != null) {
      return validationStatus;
    }
    userResourceQuotaLimit.put(userId, merged);
    mergeThrottleFromUserResource(userId, userName, merged);
    pendingThrottleMigration.remove(userId);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus deleteUserResourceQuota(DeleteUserResourceQuotaPlan plan) {
    userResourceQuotaLimit.remove(plan.getUserId());
    throttleQuotaLimit.remove(plan.getUserId());
    throttleUserNames.remove(plan.getUserId());
    pendingThrottleMigration.remove(plan.getUserId());
    if (plan.getUserName() != null) {
      legacyThrottleByUserName.remove(plan.getUserName());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /** Cascade cleanup when a user is dropped (called on the AuthorPlan apply path). */
  public void onUserDropped(long userId, String userName) {
    userResourceQuotaLimit.remove(userId);
    throttleQuotaLimit.remove(userId);
    throttleUserNames.remove(userId);
    pendingThrottleMigration.remove(userId);
    if (userName != null) {
      legacyThrottleByUserName.remove(userName);
    }
  }

  /**
   * Resolve legacy userName-keyed throttle entries into the userId-keyed map. Called after snapshot
   * load (AuthorInfo is already available) and when replaying old raft plans.
   */
  public void upgradeLegacyThrottleKeys(ToLongFunction<String> userNameToId) {
    if (legacyThrottleByUserName.isEmpty() && !mirrorAllThrottleAfterUpgrade) {
      return;
    }
    int resolved = 0;
    int unresolved = 0;
    for (Map.Entry<String, TThrottleQuota> entry :
        new HashMap<>(legacyThrottleByUserName).entrySet()) {
      long userId = userNameToId.applyAsLong(entry.getKey());
      if (userId < 0) {
        unresolved++;
        continue;
      }
      throttleQuotaLimit.put(userId, entry.getValue());
      throttleUserNames.put(userId, entry.getKey());
      legacyThrottleByUserName.remove(entry.getKey());
      if (org.apache.iotdb.commons.conf.EditionGate.isUserResourceQuotaEnabled()
          && !userResourceQuotaLimit.containsKey(userId)) {
        pendingThrottleMigration.add(userId);
      }
      resolved++;
    }
    if (mirrorAllThrottleAfterUpgrade
        && org.apache.iotdb.commons.conf.EditionGate.isUserResourceQuotaEnabled()) {
      pendingThrottleMigration.addAll(throttleQuotaLimit.keySet());
      mirrorAllThrottleAfterUpgrade = false;
    }
    if (resolved > 0 || unresolved > 0) {
      logger.info(
          ManagerMessages
              .LOG_UPGRADED_LEGACY_NAME_KEYED_THROTTLE_QUOTA_ENTRIES_TO_USERID_KEYS_RESOLVED_UNRESOLVED_1DDB7249,
          resolved,
          unresolved);
    }
  }

  /**
   * Incrementally sync derived throttle fields from the merged user resource quota. Never replace
   * the whole throttle entry, otherwise a write-only USER QUOTA update would clear prior cpu/mem.
   */
  private void mergeThrottleFromUserResource(
      long userId, String userName, TUserResourceQuota merged) {
    TThrottleQuota derived =
        UserResourceQuotaConverter.toThrottleQuota(UserResourceQuotaConverter.fromThrift(merged));
    if (userName != null) {
      throttleUserNames.put(userId, userName);
    }
    TThrottleQuota existing = throttleQuotaLimit.get(userId);
    if (existing == null) {
      throttleQuotaLimit.put(userId, derived);
      return;
    }
    if (derived.isSetCpuLimit() && derived.getCpuLimit() > 0) {
      existing.setCpuLimit(derived.getCpuLimit());
    }
    if (derived.isSetMemLimit() && derived.getMemLimit() > 0) {
      existing.setMemLimit(derived.getMemLimit());
    }
    if (derived.isSetThrottleLimit() && !derived.getThrottleLimit().isEmpty()) {
      if (!existing.isSetThrottleLimit()) {
        existing.setThrottleLimit(new HashMap<>());
      }
      existing.getThrottleLimit().putAll(derived.getThrottleLimit());
    }
  }

  /** Merge a partial SET into the persisted quota (per-resource and throttle map). */
  private TUserResourceQuota mergeUserResourceQuota(
      TUserResourceQuota existing, TUserResourceQuota incoming) {
    if (incoming.isSetReadQuota()) {
      for (Map.Entry<TResourceType, TResourceQuotaRange> entry :
          incoming.getReadQuota().entrySet()) {
        overrideRange(existing, entry.getKey(), entry.getValue(), true);
      }
    }
    if (incoming.isSetWriteQuota()) {
      for (Map.Entry<TResourceType, TResourceQuotaRange> entry :
          incoming.getWriteQuota().entrySet()) {
        overrideRange(existing, entry.getKey(), entry.getValue(), false);
      }
    }
    if (incoming.isSetThrottleLimit()) {
      if (!existing.isSetThrottleLimit()) {
        existing.setThrottleLimit(new HashMap<>());
      }
      for (Map.Entry<ThrottleType, TTimedQuota> entry : incoming.getThrottleLimit().entrySet()) {
        existing.getThrottleLimit().put(entry.getKey(), entry.getValue());
      }
    }
    return existing;
  }

  private TSStatus validateUserResourceQuota(TUserResourceQuota quota) {
    TSStatus status = validateRanges(quota.getReadQuota(), OperationType.READ);
    return status != null ? status : validateRanges(quota.getWriteQuota(), OperationType.WRITE);
  }

  private TSStatus validateRanges(
      Map<TResourceType, TResourceQuotaRange> ranges, OperationType operation) {
    if (ranges == null) {
      return null;
    }
    for (Map.Entry<TResourceType, TResourceQuotaRange> entry : ranges.entrySet()) {
      TResourceQuotaRange range = entry.getValue();
      if (range != null
          && range.getMinValue() != IoTDBConstant.UNLIMITED_VALUE
          && range.getMaxValue() != IoTDBConstant.UNLIMITED_VALUE
          && range.getMinValue() > range.getMaxValue()) {
        return RpcUtils.getStatus(
            TSStatusCode.SEMANTIC_ERROR,
            String.format(
                ManagerMessages
                    .MESSAGE_INVALID_USER_QUOTA_RANGE_FOR_ARG_ARG_MIN_ARG_MUST_NOT_EXCEED_MAX_ARG_2B8D90C9,
                operation.name().toLowerCase(Locale.ROOT),
                entry.getKey().name().toLowerCase(Locale.ROOT),
                range.getMinValue(),
                range.getMaxValue()));
      }
    }
    return null;
  }

  /**
   * Override one resource min/max into read or write side; unset bounds in the incoming range keep
   * the previous value.
   */
  private void overrideRange(
      TUserResourceQuota existing, TResourceType type, TResourceQuotaRange incoming, boolean read) {
    TResourceQuotaRange current =
        read
            ? (existing.isSetReadQuota() ? existing.getReadQuota().get(type) : null)
            : (existing.isSetWriteQuota() ? existing.getWriteQuota().get(type) : null);
    if (current == null) {
      current =
          new TResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, IoTDBConstant.UNLIMITED_VALUE);
    }
    if (incoming.getMinValue() != IoTDBConstant.UNLIMITED_VALUE) {
      current.setMinValue(incoming.getMinValue());
    }
    if (incoming.getMaxValue() != IoTDBConstant.UNLIMITED_VALUE) {
      current.setMaxValue(incoming.getMaxValue());
    }
    if (read) {
      if (!existing.isSetReadQuota()) {
        existing.setReadQuota(new HashMap<>());
      }
      existing.getReadQuota().put(type, current);
    } else {
      if (!existing.isSetWriteQuota()) {
        existing.setWriteQuota(new HashMap<>());
      }
      existing.getWriteQuota().put(type, current);
    }
  }

  public Map<String, TSpaceQuota> getSpaceQuotaLimit() {
    return spaceQuotaLimit;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      logger.error(
          ManagerMessages.LOG_FAILED_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_ARG_ALREADY_EXIST_EB2A6093,
          snapshotFile.getAbsolutePath());
      return false;
    }

    spaceQuotaReadWriteLock.writeLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      serializeSpaceQuotaLimit(fileOutputStream);
      serializeThrottleQuotaLimit(fileOutputStream);
      serializeUserResourceQuotaLimit(fileOutputStream);
      fileOutputStream.getFD().sync();
    } finally {
      spaceQuotaReadWriteLock.writeLock().unlock();
    }
    return true;
  }

  private void serializeSpaceQuotaLimit(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(spaceQuotaLimit.size(), fileOutputStream);
    for (Map.Entry<String, TSpaceQuota> spaceQuotaEntry : spaceQuotaLimit.entrySet()) {
      ReadWriteIOUtils.write(spaceQuotaEntry.getKey(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getDeviceNum(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getTimeserieNum(), fileOutputStream);
      ReadWriteIOUtils.write(spaceQuotaEntry.getValue().getDiskSize(), fileOutputStream);
    }
  }

  private void serializeThrottleQuotaLimit(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(THROTTLE_QUOTA_USER_ID_FORMAT, fileOutputStream);
    // Persist upgraded entries plus any still-unresolved legacy name-keyed rows (as userId=-1 is
    // not used; unresolved stay only in legacyThrottleByUserName and are not snapshotted — they
    // must be upgraded before takeSnapshot in normal operation). Persist legacy map after.
    ReadWriteIOUtils.write(throttleQuotaLimit.size(), fileOutputStream);
    for (Map.Entry<Long, TThrottleQuota> throttleQuotaEntry : throttleQuotaLimit.entrySet()) {
      long userId = throttleQuotaEntry.getKey();
      ReadWriteIOUtils.write(userId, fileOutputStream);
      String userName = throttleUserNames.getOrDefault(userId, "");
      ReadWriteIOUtils.write(userName, fileOutputStream);
      writeThrottleQuota(throttleQuotaEntry.getValue(), fileOutputStream);
    }
    ReadWriteIOUtils.write(legacyThrottleByUserName.size(), fileOutputStream);
    for (Map.Entry<String, TThrottleQuota> entry : legacyThrottleByUserName.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      writeThrottleQuota(entry.getValue(), fileOutputStream);
    }
  }

  private static void writeThrottleQuota(TThrottleQuota throttleQuota, FileOutputStream out)
      throws IOException {
    ReadWriteIOUtils.write(throttleQuota.getThrottleLimit().size(), out);
    for (Map.Entry<ThrottleType, TTimedQuota> entry : throttleQuota.getThrottleLimit().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().name(), out);
      ReadWriteIOUtils.write(entry.getValue().getTimeUnit(), out);
      ReadWriteIOUtils.write(entry.getValue().getSoftLimit(), out);
    }
    ReadWriteIOUtils.write(throttleQuota.getMemLimit(), out);
    ReadWriteIOUtils.write(throttleQuota.getCpuLimit(), out);
  }

  private static TThrottleQuota readThrottleQuota(FileInputStream in) throws IOException {
    int quotaSize = ReadWriteIOUtils.readInt(in);
    Map<ThrottleType, TTimedQuota> quotaLimit = new HashMap<>();
    while (quotaSize > 0) {
      ThrottleType throttleType = ThrottleType.valueOf(ReadWriteIOUtils.readString(in));
      long timeUnit = ReadWriteIOUtils.readLong(in);
      long softLimit = ReadWriteIOUtils.readLong(in);
      quotaLimit.put(throttleType, new TTimedQuota(timeUnit, softLimit));
      quotaSize--;
    }
    TThrottleQuota throttleQuota = new TThrottleQuota();
    throttleQuota.setThrottleLimit(quotaLimit);
    throttleQuota.setMemLimit(ReadWriteIOUtils.readLong(in));
    throttleQuota.setCpuLimit(ReadWriteIOUtils.readInt(in));
    return throttleQuota;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      logger.error(
          ManagerMessages.LOG_FAILED_LOAD_SNAPSHOT_SNAPSHOT_FILE_ARG_NOT_EXIST_8828CFBA,
          snapshotFile.getAbsolutePath());
      return;
    }
    spaceQuotaReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      clear();
      deserializeSpaceQuotaLimit(fileInputStream);
      deserializeThrottleQuotaLimit(fileInputStream);
      if (fileInputStream.available() > 0) {
        deserializeUserResourceQuotaLimit(fileInputStream);
      } else if (org.apache.iotdb.commons.conf.EditionGate.isUserResourceQuotaEnabled()) {
        // Legacy snapshot without the user resource quota section: mirror after userId upgrade.
        mirrorAllThrottleAfterUpgrade = true;
      }
    } finally {
      spaceQuotaReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeSpaceQuotaLimit(FileInputStream fileInputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(fileInputStream);
    while (size > 0) {
      String path = ReadWriteIOUtils.readString(fileInputStream);
      TSpaceQuota spaceQuota = new TSpaceQuota();
      spaceQuota.setDeviceNum(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuota.setTimeserieNum(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuota.setDiskSize(ReadWriteIOUtils.readLong(fileInputStream));
      spaceQuotaLimit.put(path, spaceQuota);
      spaceQuotaUsage.put(path, new TSpaceQuota());
      size--;
    }
  }

  private void deserializeThrottleQuotaLimit(FileInputStream fileInputStream) throws IOException {
    int markerOrSize = ReadWriteIOUtils.readInt(fileInputStream);
    if (markerOrSize == THROTTLE_QUOTA_USER_ID_FORMAT) {
      int size = ReadWriteIOUtils.readInt(fileInputStream);
      while (size > 0) {
        long userId = ReadWriteIOUtils.readLong(fileInputStream);
        String userName = ReadWriteIOUtils.readString(fileInputStream);
        TThrottleQuota throttleQuota = readThrottleQuota(fileInputStream);
        throttleQuotaLimit.put(userId, throttleQuota);
        if (userName != null && !userName.isEmpty()) {
          throttleUserNames.put(userId, userName);
        }
        size--;
      }
      int legacySize = ReadWriteIOUtils.readInt(fileInputStream);
      while (legacySize > 0) {
        String userName = ReadWriteIOUtils.readString(fileInputStream);
        legacyThrottleByUserName.put(userName, readThrottleQuota(fileInputStream));
        legacySize--;
      }
      return;
    }
    // Legacy userName-keyed format: first int was the map size.
    int size = markerOrSize;
    while (size > 0) {
      String userName = ReadWriteIOUtils.readString(fileInputStream);
      legacyThrottleByUserName.put(userName, readThrottleQuota(fileInputStream));
      size--;
    }
  }

  public Map<String, TSpaceQuota> getSpaceQuotaUsage() {
    return spaceQuotaUsage;
  }

  public Map<Long, TThrottleQuota> getThrottleQuotaLimit() {
    return throttleQuotaLimit;
  }

  public Map<Long, String> getThrottleUserNames() {
    return throttleUserNames;
  }

  /** Test / upgrade hook: unresolved legacy name-keyed throttle rows. */
  public Map<String, TThrottleQuota> getLegacyThrottleByUserName() {
    return legacyThrottleByUserName;
  }

  public Map<Long, TUserResourceQuota> getUserResourceQuotaLimit() {
    return userResourceQuotaLimit;
  }

  public Set<Long> getPendingThrottleMigration() {
    return pendingThrottleMigration;
  }

  private void serializeUserResourceQuotaLimit(FileOutputStream fileOutputStream)
      throws IOException {
    ReadWriteIOUtils.write(userResourceQuotaLimit.size(), fileOutputStream);
    for (Map.Entry<Long, TUserResourceQuota> entry : userResourceQuotaLimit.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      writeUserResourceQuota(entry.getValue(), fileOutputStream);
    }
  }

  /** Load userId-keyed USER QUOTA. Pending throttle migration is rebuilt in memory after load. */
  private void deserializeUserResourceQuotaLimit(FileInputStream fileInputStream)
      throws IOException {
    int size = ReadWriteIOUtils.readInt(fileInputStream);
    while (size > 0) {
      long userId = ReadWriteIOUtils.readLong(fileInputStream);
      userResourceQuotaLimit.put(userId, readUserResourceQuota(fileInputStream));
      size--;
    }
  }

  private void writeUserResourceQuota(TUserResourceQuota quota, FileOutputStream stream)
      throws IOException {
    writeRangeMap(quota.isSetReadQuota() ? quota.getReadQuota() : new HashMap<>(), stream);
    writeRangeMap(quota.isSetWriteQuota() ? quota.getWriteQuota() : new HashMap<>(), stream);
    Map<ThrottleType, TTimedQuota> throttleLimit =
        quota.isSetThrottleLimit() ? quota.getThrottleLimit() : new HashMap<>();
    ReadWriteIOUtils.write(throttleLimit.size(), stream);
    for (Map.Entry<ThrottleType, TTimedQuota> entry : throttleLimit.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().name(), stream);
      ReadWriteIOUtils.write(entry.getValue().getTimeUnit(), stream);
      ReadWriteIOUtils.write(entry.getValue().getSoftLimit(), stream);
    }
  }

  private TUserResourceQuota readUserResourceQuota(FileInputStream stream) throws IOException {
    TUserResourceQuota quota = new TUserResourceQuota();
    quota.setReadQuota(readRangeMap(stream));
    quota.setWriteQuota(readRangeMap(stream));
    int throttleSize = ReadWriteIOUtils.readInt(stream);
    Map<ThrottleType, TTimedQuota> throttleLimit = new HashMap<>();
    while (throttleSize > 0) {
      ThrottleType type = ThrottleType.valueOf(ReadWriteIOUtils.readString(stream));
      long timeUnit = ReadWriteIOUtils.readLong(stream);
      long softLimit = ReadWriteIOUtils.readLong(stream);
      throttleLimit.put(type, new TTimedQuota(timeUnit, softLimit));
      throttleSize--;
    }
    quota.setThrottleLimit(throttleLimit);
    return quota;
  }

  private void writeRangeMap(
      Map<TResourceType, TResourceQuotaRange> rangeMap, FileOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(rangeMap.size(), stream);
    for (Map.Entry<TResourceType, TResourceQuotaRange> entry : rangeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().name(), stream);
      ReadWriteIOUtils.write(entry.getValue().getMinValue(), stream);
      ReadWriteIOUtils.write(entry.getValue().getMaxValue(), stream);
    }
  }

  private Map<TResourceType, TResourceQuotaRange> readRangeMap(FileInputStream stream)
      throws IOException {
    int size = ReadWriteIOUtils.readInt(stream);
    Map<TResourceType, TResourceQuotaRange> map = new HashMap<>();
    while (size > 0) {
      TResourceType type = TResourceType.valueOf(ReadWriteIOUtils.readString(stream));
      long min = ReadWriteIOUtils.readLong(stream);
      long max = ReadWriteIOUtils.readLong(stream);
      map.put(type, new TResourceQuotaRange(min, max));
      size--;
    }
    return map;
  }

  public void clear() {
    spaceQuotaLimit.clear();
    spaceQuotaUsage.clear();
    throttleQuotaLimit.clear();
    throttleUserNames.clear();
    legacyThrottleByUserName.clear();
    userResourceQuotaLimit.clear();
    pendingThrottleMigration.clear();
    mirrorAllThrottleAfterUpgrade = false;
  }
}
