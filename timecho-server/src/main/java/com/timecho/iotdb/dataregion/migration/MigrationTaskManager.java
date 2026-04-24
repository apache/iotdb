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

package com.timecho.iotdb.dataregion.migration;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor.IMigrationManager;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import com.google.common.util.concurrent.RateLimiter;
import com.timecho.iotdb.metrics.MigrationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MigrationTaskManager implements IService, IMigrationManager {
  private static final Logger logger = LoggerFactory.getLogger(MigrationTaskManager.class);
  private static final IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();
  private static final long CHECK_INTERVAL_IN_SECONDS = 10;

  /** enable or not */
  private boolean enable = false;

  /** single thread to schedule */
  private ScheduledExecutorService scheduler;

  /** workers to migrate files */
  private WrappedThreadPoolExecutor workers;

  /** migrate rate limiter, KB/s */
  private volatile RateLimiter[] migrateRateLimiters;

  @Override
  public void start() throws StartupException {
    enable = true;
    // metrics
    MetricService.getInstance().addMetricSet(MigrationMetrics.getInstance());
    // threads and tasks
    reloadMigrateSpeedLimit();
    scheduler =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.MIGRATION_SCHEDULER.getName());
    workers =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                iotdbConfig.getMigrateThreadCount(), ThreadName.MIGRATION.getName());
    workers.disableErrorLog();
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        () -> new MigrationScheduleTask().run(),
        CHECK_INTERVAL_IN_SECONDS,
        CHECK_INTERVAL_IN_SECONDS,
        TimeUnit.SECONDS);
  }

  private class MigrationScheduleTask implements Runnable {
    private final long[] tierDiskTotalSpace = tierManager.getTierDiskTotalSpace();
    private final long[] tierDiskUsableSpace = tierManager.getTierDiskUsableSpace();

    /** Tiers need migrating data to the next tier */
    private final Set<Integer> needMigrationTiers = new HashSet<>();

    /** Tiers are disk-full, cannot migrate data to these tiers */
    private final Set<Integer> spaceWarningTiers = new HashSet<>();

    /** TsFiles of all data region except for audit */
    private final List<TsFileResource> tsfiles = new ArrayList<>();

    /** TsFiles of all data region for audit */
    private final List<TsFileResource> auditTsFiles = new ArrayList<>();

    /** Audit TsFiles Total Space */
    private long auditTsFilesTotalSpaceInBytes = 0L;

    /** Audit TsFiles Max Space */
    private final double auditTsFilesMaxSpaceInGB = commonConfig.getAuditLogSpaceTlInGB();

    public MigrationScheduleTask() {
      for (int i = 0; i < tierManager.getTiersNum(); i++) {
        double usable = tierDiskUsableSpace[i] * 1.0 / tierDiskTotalSpace[i];
        if (usable <= 1 - iotdbConfig.getSpaceUsageThresholds()[i]) {
          needMigrationTiers.add(i);
        }
        if (usable <= commonConfig.getDiskSpaceWarningThreshold()) {
          spaceWarningTiers.add(i);
        }
      }
    }

    private boolean isLastTier(int tierLevel) {
      return tierLevel + 1 >= tierDiskTotalSpace.length;
    }

    private void releaseDiskUsage(int tierLevel, long usage) {
      tierDiskUsableSpace[tierLevel] += usage;
      if (needMigrationTiers.contains(tierLevel)) {
        double usable = tierDiskUsableSpace[tierLevel] * 1.0 / tierDiskTotalSpace[tierLevel];
        if (usable > 1 - iotdbConfig.getSpaceUsageThresholds()[tierLevel]) {
          needMigrationTiers.remove(tierLevel);
        }
      }
    }

    @Override
    public void run() {
      schedule();
    }

    private void schedule() {
      for (DataRegion dataRegion : StorageEngine.getInstance().getAllDataRegions()) {
        if (dataRegion.getDatabaseName().startsWith(SystemConstant.AUDIT_DATABASE)) {
          auditTsFiles.addAll(dataRegion.getSequenceFileList());
          auditTsFiles.addAll(dataRegion.getUnSequenceFileList());
          continue;
        }
        tsfiles.addAll(dataRegion.getSequenceFileList());
        tsfiles.addAll(dataRegion.getUnSequenceFileList());
      }
      scheduleAuditDeletion();
      if (TierFullPolicy.valueOf(iotdbConfig.getTierFullPolicy()) == TierFullPolicy.DELETE) {
        scheduleDeletion();
      }
      if (iotdbConfig.getTierDataDirs().length != 1) {
        scheduleMigration();
      }
    }

    private void scheduleAuditDeletion() {
      if (auditTsFilesMaxSpaceInGB == Double.MAX_VALUE) {
        return;
      }
      if (auditTsFiles.isEmpty()) {
        return;
      }
      // calculate audit TsFiles total space
      for (TsFileResource tsFileResource : auditTsFiles) {
        if (tsFileResource.getStatus() == TsFileResourceStatus.NORMAL) {
          auditTsFilesTotalSpaceInBytes += tsFileResource.getTsFileSize();
        }
      }

      // if audit TsFiles total space is under threshold, return
      if (auditTsFilesTotalSpaceInBytes * 1.0 / (1024L * 1024L * 1024L)
          <= auditTsFilesMaxSpaceInGB) {
        return;
      }
      AuditLogFields fields =
          new AuditLogFields(
              AuthorityChecker.INTERNAL_CONTROL_USER_ID,
              AuthorityChecker.INTERNAL_CONTROL_USER,
              null,
              AuditEventType.AUDIT_STORAGE_FULL,
              AuditLogOperation.CONTROL,
              PrivilegeType.AUDIT,
              true,
              null,
              null);
      String message =
          String.format(
              "Audit log storage is full, total space: %.2f GB, max space: %.2f GB. Start deleting old audit logs.",
              auditTsFilesTotalSpaceInBytes * 1.0 / (1024L * 1024L * 1024L),
              auditTsFilesMaxSpaceInGB);
      DNAuditLogger.getInstance().log(fields, () -> message);
      List<TsFileResource> deleteCandidates =
          auditTsFiles.stream()
              .filter(f -> f.getStatus() == TsFileResourceStatus.NORMAL)
              .sorted(this::compareDeletePriority)
              .collect(Collectors.toList());
      // submit delete tasks
      for (TsFileResource tsfile : deleteCandidates) {
        if (auditTsFilesTotalSpaceInBytes * 1.0 / (1024L * 1024L * 1024L)
            <= auditTsFilesMaxSpaceInGB) {
          return;
        }
        trySubmitAuditDeleteTask(tsfile.getTierLevel(), tsfile);
      }
    }

    private void scheduleDeletion() {
      if (!needMigrationTiers.contains(tierDiskTotalSpace.length - 1)) {
        return;
      }
      // only delete files in the last tier
      List<TsFileResource> deleteCandidates =
          tsfiles.stream()
              .filter(
                  f -> f.getStatus() == TsFileResourceStatus.NORMAL && isLastTier(f.getTierLevel()))
              .sorted(this::compareDeletePriority)
              .collect(Collectors.toList());
      // submit delete tasks
      for (TsFileResource tsfile : deleteCandidates) {
        if (!needMigrationTiers.contains(tierDiskTotalSpace.length - 1)) {
          break;
        }
        trySubmitDeleteTask(tierDiskTotalSpace.length - 1, tsfile);
      }
    }

    private void trySubmitDeleteTask(int tierLevel, TsFileResource sourceTsFile) {
      if (!sourceTsFile.setStatus(TsFileResourceStatus.MIGRATING)) {
        return;
      }
      releaseDiskUsage(tierLevel, sourceTsFile.getTsFileSize());
      workers.submit(new DeleteTask(sourceTsFile));
    }

    private void trySubmitAuditDeleteTask(int tierLevel, TsFileResource sourceTsFile) {
      if (!sourceTsFile.setStatus(TsFileResourceStatus.MIGRATING)) {
        return;
      }
      releaseDiskUsage(tierLevel, sourceTsFile.getTsFileSize());
      auditTsFilesTotalSpaceInBytes -= sourceTsFile.getTsFileSize();
      workers.submit(new DeleteTask(sourceTsFile));
    }

    private int compareDeletePriority(TsFileResource f1, TsFileResource f2) {
      // old time partitions first
      int res = Long.compare(f1.getTimePartition(), f2.getTimePartition());
      // old file first
      if (res == 0) {
        res = Long.compare(f1.getVersion(), f2.getVersion());
      }
      return res;
    }

    private void scheduleMigration() {
      // only migrate closed TsFiles not in the last tier
      List<TsFileResource> migrateCandidates =
          tsfiles.stream()
              .filter(
                  f ->
                      f.getStatus() == TsFileResourceStatus.NORMAL && !isLastTier(f.getTierLevel()))
              .sorted(this::compareMigrationPriority)
              .collect(Collectors.toList());
      // submit migration tasks
      for (TsFileResource tsfile : migrateCandidates) {
        try {
          int currentTier = tsfile.getTierLevel();
          int nextTier = currentTier + 1;
          // skip migration when next tier is full
          if (spaceWarningTiers.contains(nextTier)) {
            continue;
          }
          // check tier ttl and disk space
          boolean stillLives = true;
          long currentTierTTLInMs = commonConfig.getTierTTLInMs()[currentTier];
          if (currentTierTTLInMs != Long.MAX_VALUE) {
            long lowerBound =
                DateTimeUtils.convertMilliTimeWithPrecision(
                    System.currentTimeMillis() - currentTierTTLInMs,
                    commonConfig.getTimestampPrecision());
            stillLives = tsfile.stillLives(lowerBound);
          }
          if (!stillLives) {
            trySubmitMigrationTask(
                currentTier,
                MigrationCause.TTL,
                tsfile,
                tierManager.getNextFolderForTsFile(nextTier, tsfile.isSeq()));
          } else if (needMigrationTiers.contains(currentTier)) {
            trySubmitMigrationTask(
                currentTier,
                MigrationCause.DISK_SPACE,
                tsfile,
                tierManager.getNextFolderForTsFile(nextTier, tsfile.isSeq()));
          }
        } catch (Exception e) {
          logger.error(
              "An error occurred when check and try to migrate TsFileResource {}", tsfile, e);
        }
      }
    }

    private void trySubmitMigrationTask(
        int tierLevel, MigrationCause cause, TsFileResource sourceTsFile, String targetDir)
        throws IOException {
      if (!sourceTsFile.setStatus(TsFileResourceStatus.MIGRATING)) {
        return;
      }
      releaseDiskUsage(tierLevel, sourceTsFile.getTsFileSize());
      workers.submit(MigrationTask.newTask(cause, sourceTsFile, targetDir));
    }

    private int compareMigrationPriority(TsFileResource f1, TsFileResource f2) {
      // lower tier first
      int res = Integer.compare(f1.getTierLevel(), f2.getTierLevel());
      // old time partitions first
      if (res == 0) {
        res = Long.compare(f1.getTimePartition(), f2.getTimePartition());
      }
      // sequence files in one partition
      if (res == 0) {
        if (f1.isSeq() && !f2.isSeq()) {
          res = -1;
        } else if (!f1.isSeq() && f2.isSeq()) {
          res = 1;
        }
      }
      // old version files in one partition
      if (res == 0) {
        res = Long.compare(f1.getVersion(), f2.getVersion());
      }
      return res;
    }
  }

  void acquireMigrateSpeedLimiter(int tierLevel, long size) {
    long sizeInKb = size / 1024 + 1;
    while (sizeInKb > 0) {
      if (sizeInKb > Integer.MAX_VALUE) {
        migrateRateLimiters[tierLevel].acquire(Integer.MAX_VALUE);
        sizeInKb -= Integer.MAX_VALUE;
      } else {
        migrateRateLimiters[tierLevel].acquire((int) sizeInKb);
        return;
      }
    }
  }

  @Override
  public void reloadMigrateSpeedLimit() {
    long[] limitRates = iotdbConfig.getTieredStorageMigrateSpeedLimitBytesPerSec();
    if (limitRates == null) {
      migrateRateLimiters = null;
      return;
    }
    RateLimiter[] newRateLimiters = new RateLimiter[limitRates.length];
    for (int i = 0; i < newRateLimiters.length; ++i) {
      newRateLimiters[i] =
          RateLimiter.create(limitRates[i] <= 0 ? Double.MAX_VALUE : (double) limitRates[i] / 1024);
    }
    migrateRateLimiters = newRateLimiters;
  }

  @Override
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (workers != null) {
      workers.shutdownNow();
    }
    enable = false;
  }

  @Override
  public boolean isEnable() {
    return enable;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MIGRATION_SERVICE;
  }

  public static MigrationTaskManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final MigrationTaskManager INSTANCE = new MigrationTaskManager();
  }
}
