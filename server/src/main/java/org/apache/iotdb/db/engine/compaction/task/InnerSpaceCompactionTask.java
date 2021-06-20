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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.WriteLockFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(InnerSpaceCompactionTask.class);
  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResourceList tsFileResourceList;
  protected boolean sequence;
  protected Set<String> skippedDevicesSet;
  public static final String fileNameRegex = "([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)";

  public InnerSpaceCompactionTask(CompactionContext context) {
    super(
        context.getStorageGroupName() + "-" + context.getVirtualStorageGroupName(),
        context.getTimePartitionId());
    this.tsFileResourceList =
        context.isSequence()
            ? context.getSequenceFileResourceList()
            : context.getUnsequenceFileResourceList();
    this.selectedTsFileResourceList =
        context.isSequence()
            ? context.getSelectedSequenceFiles()
            : context.getSelectedUnsequenceFiles();
    this.sequence = context.isSequence();
    this.skippedDevicesSet = new HashSet<>();
  }

  @Override
  protected void doCompaction() throws Exception {
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String targetFileName = generateTargetFileName(selectedTsFileResourceList);
    TsFileResource targetTsFileResource =
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
    LOGGER.info(
        "{} [Compaction] starting compaction task with {} files",
        storageGroupName,
        selectedTsFileResourceList.size());
    try {
      File logFile =
          new File(dataDirectory + File.separator + targetFileName + COMPACTION_LOG_SUFFIX);
      // compaction execution
      List<Modification> modifications = new ArrayList<>();
      CompactionLogger compactionLogger = new CompactionLogger(logFile.getPath());
      for (TsFileResource resource : selectedTsFileResourceList) {
        compactionLogger.logFile(SOURCE_NAME, resource.getTsFile());
      }
      compactionLogger.logSequence(sequence);
      compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
      LOGGER.info(
          "{} [Compaction] compaction with {}", storageGroupName, selectedTsFileResourceList);
      CompactionUtils.compact(
          targetTsFileResource,
          selectedTsFileResourceList,
          storageGroupName,
          compactionLogger,
          this.skippedDevicesSet,
          sequence,
          modifications);
      compactionLogger.close();
      if (logFile.exists()) {
        logFile.delete();
      }
    } finally {
      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.readUnlock();
      }
    }
    LOGGER.info("{} [Compaction] compaction finish, start to delete old files", storageGroupName);
    try {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException(String.format("%s [Compaction] abort", storageGroupName));
      }
      tsFileResourceList.writeLockWithTimeout(5000);
    } catch (WriteLockFailedException e) {
      // if the thread of time partition deletion get the write lock,
      // current thread will catch a WriteLockFailException, then terminate the thread itself
      throw new InterruptedException(
          String.format(
              "%s [Compaction] compaction abort because cannot acquire write lock",
              storageGroupName));
    }
    try {
      // replace the old files with new file, the new is in same position as the old
      tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
      for (TsFileResource resource : selectedTsFileResourceList) {
        tsFileResourceList.remove(resource);
      }
      // delete the old files
      CompactionUtils.deleteTsFilesInDisk(selectedTsFileResourceList, storageGroupName);
    } finally {
      tsFileResourceList.writeUnlock();
    }
  }

  /**
   * This method receive a list of TsFileResource to be compacted, return the name of compaction
   * target file.
   *
   * @return target file name as {minTimestamp}-{minVersionNum}-{maxInnerMergeTimes +
   *     1}-{maxCrossMergeTimes}
   */
  public String generateTargetFileName(List<TsFileResource> tsFileResourceList) {
    long minTimestamp = Long.MAX_VALUE;
    long minVersionNum = Long.MAX_VALUE;
    int maxInnerMergeTimes = Integer.MIN_VALUE;
    int maxCrossMergeTimes = Integer.MIN_VALUE;
    Pattern tsFilePattern = Pattern.compile(fileNameRegex);

    for (TsFileResource resource : tsFileResourceList) {
      String tsFileName = resource.getTsFile().getName();
      Matcher matcher = tsFilePattern.matcher(tsFileName);
      if (matcher.find()) {
        long currentTimestamp = Long.parseLong(matcher.group(1));
        long currentVersionNum = Long.parseLong(matcher.group(2));
        int currentInnerMergeTimes = Integer.parseInt(matcher.group(3));
        int currentCrossMergeTimes = Integer.parseInt(matcher.group(4));
        minTimestamp = Math.min(minTimestamp, currentTimestamp);
        minVersionNum = Math.min(minVersionNum, currentVersionNum);
        maxInnerMergeTimes = Math.max(maxInnerMergeTimes, currentInnerMergeTimes);
        maxCrossMergeTimes = Math.max(maxCrossMergeTimes, currentCrossMergeTimes);
      }
    }

    return TsFileNameGenerator.generateNewTsFileName(
        minTimestamp, minVersionNum, maxInnerMergeTimes + 1, maxCrossMergeTimes);
  }
}
