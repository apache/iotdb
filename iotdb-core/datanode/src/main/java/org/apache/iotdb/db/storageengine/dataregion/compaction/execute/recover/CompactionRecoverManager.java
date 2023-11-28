/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InsertionCrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.conf.IoTDBConstant.BLANK;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MODS_SETTLE_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.SETTLE_SUFFIX;

/**
 * CompactionRecoverManager searches compaction log and call {@link CompactionRecoverTask} to
 * execute the recover process for all compaction task sequentially, including InnerCompactionTask
 * in sequence/unsequence space, CrossSpaceCompaction.
 */
public class CompactionRecoverManager {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final TsFileManager tsFileManager;
  private final String logicalStorageGroupName;
  private final String dataRegionId;

  public CompactionRecoverManager(
      TsFileManager tsFileManager, String logicalStorageGroupName, String dataRegionId) {
    this.tsFileManager = tsFileManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.dataRegionId = dataRegionId;
  }

  public void recoverInnerSpaceCompaction(boolean isSequence) {
    logger.info(
        "{} [Compaction][Recover] recovering {} inner compaction",
        logicalStorageGroupName,
        isSequence ? "sequence" : "unsequence");
    recoverCompaction(true, isSequence);
  }

  public void recoverCrossSpaceCompaction() {
    logger.info("{} [Compaction][Recover] recovering cross compaction", logicalStorageGroupName);
    recoverCompaction(false, true);
  }

  @SuppressWarnings("squid:S3776")
  private void recoverCompaction(boolean isInnerSpace, boolean isLogSequence) {
    List<String> dirs;
    if (isLogSequence) {
      dirs = TierManager.getInstance().getAllLocalSequenceFileFolders();
    } else {
      dirs = TierManager.getInstance().getAllLocalUnSequenceFileFolders();
    }
    for (String dir : dirs) {
      File storageGroupDir =
          new File(dir + File.separator + logicalStorageGroupName + File.separator + dataRegionId);
      logger.info(
          "{} [Compaction][Recover] recover compaction in data region dir {}",
          logicalStorageGroupName,
          storageGroupDir.getAbsolutePath());
      if (!storageGroupDir.exists()) {
        return;
      }
      File[] timePartitionDirs = storageGroupDir.listFiles();
      if (timePartitionDirs == null) {
        return;
      }
      for (File timePartitionDir : timePartitionDirs) {
        if (!timePartitionDir.isDirectory()
            || !Pattern.compile("\\d*").matcher(timePartitionDir.getName()).matches()) {
          continue;
        }
        logger.info(
            "{} [Compaction][Recover] recover compaction in time partition dir {}",
            logicalStorageGroupName,
            timePartitionDir.getAbsolutePath());
        // recover temporary files generated during compacted
        recoverCompaction(isInnerSpace, timePartitionDir);

        // recover temporary files generated during .mods file settled
        recoverModSettleFile(timePartitionDir.toPath());
      }
    }
  }

  public void recoverModSettleFile(Path timePartitionDir) {
    try (Stream<Path> settlesStream = Files.list(timePartitionDir)) {
      settlesStream
          .filter(path -> path.toString().endsWith(MODS_SETTLE_FILE_SUFFIX))
          .forEach(
              modsSettle -> {
                Path originModFile =
                    modsSettle.resolveSibling(
                        modsSettle.getFileName().toString().replace(SETTLE_SUFFIX, BLANK));
                try {
                  if (Files.exists(originModFile)) {
                    Files.deleteIfExists(modsSettle);
                  } else {
                    Files.move(modsSettle, originModFile);
                  }
                } catch (IOException e) {
                  logger.error(
                      "recover mods file error on delete origin file or rename mods settle,", e);
                }
              });
    } catch (IOException e) {
      logger.error("recover mods file error on list files:{}", timePartitionDir, e);
    }
  }

  public void recoverCompaction(boolean isInnerSpace, File timePartitionDir) {
    File[] compactionLogs =
        CompactionLogger.findCompactionLogs(isInnerSpace, timePartitionDir.getPath());
    for (File compactionLog : compactionLogs) {
      logger.info(
          "{} [Compaction][Recover] calling compaction recover task.", logicalStorageGroupName);
      if (!isInnerSpace
          && compactionLog
              .getAbsolutePath()
              .endsWith(CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX)) {
        new InsertionCrossSpaceCompactionTask(
                logicalStorageGroupName, dataRegionId, tsFileManager, compactionLog)
            .recover();
      } else {
        new CompactionRecoverTask(
                logicalStorageGroupName, dataRegionId, tsFileManager, compactionLog, isInnerSpace)
            .doCompaction();
      }
    }
  }
}
