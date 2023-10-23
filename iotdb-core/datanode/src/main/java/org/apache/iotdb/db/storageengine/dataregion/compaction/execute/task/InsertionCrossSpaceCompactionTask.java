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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InsertionCrossSpaceCompactionTask extends AbstractCompactionTask {

  private Phaser phaser;

  public InsertionCrossSpaceCompactionTask(
      Phaser phaser,
      long timePartition,
      TsFileManager tsFileManager,
      InsertionCrossCompactionTaskResource taskResource,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.phaser = phaser;
    this.selectedSeqFiles = new ArrayList<>();
    if (taskResource.prevSeqFile != null) {
      selectedSeqFiles.add(taskResource.prevSeqFile);
    }
    if (taskResource.nextSeqFile != null) {
      selectedSeqFiles.add(taskResource.nextSeqFile);
    }
    this.unseqFileToInsert = taskResource.toInsertUnSeqFile;
    this.selectedUnseqFiles = Collections.singletonList(unseqFileToInsert);
  }

  private TsFileResource unseqFileToInsert;
  private TsFileResource targetFile;
  private long timestamp;

  private List<TsFileResource> selectedSeqFiles;
  private List<TsFileResource> selectedUnseqFiles;

  @Override
  protected List<TsFileResource> getAllSourceTsFiles() {
    return Stream.concat(selectedSeqFiles.stream(), selectedUnseqFiles.stream()).collect(Collectors.toList());
  }

  @Override
  public void handleTaskCleanup() {
    phaser.arrive();
  }

  @Override
  protected boolean doCompaction() {

    // 1. 日志记录任务相关的文件
    // 2. TsFileManager 中移动到顺序区(不能使用 keepOrderInsert，因为还没有改名，此时排序插入会出错, 可以直接使用 insertBefore 或
    // insertAfter )
    // 3. 源 TsFileResource 加写锁
    // 4. rename 源乱序 TsFileResource 到目标位置
    // 5. 释放写锁

    // log
    boolean isSuccess = true;
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    File logFile =
        new File(
            unseqFileToInsert.getTsFilePath()
                + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {
      targetFile = new TsFileResource(generateTargetFile());

      logger.logSourceFiles(Collections.singletonList(unseqFileToInsert));
      logger.logTargetFile(targetFile);
      logger.force();

      prepareTargetFiles();

      // todo: overlap validation

      replaceTsFileInMemory(
          Collections.singletonList(unseqFileToInsert), Collections.singletonList(targetFile));

      CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
          Collections.singletonList(unseqFileToInsert), false);
      CompactionUtils.deleteCompactionModsFile(selectedSeqFiles, selectedUnseqFiles);

      if (!targetFile.isDeleted()) {
        FileMetrics.getInstance()
            .addTsFile(
                targetFile.getDatabaseName(),
                targetFile.getDataRegionId(),
                targetFile.getTsFileSize(),
                true,
                targetFile.getTsFile().getName());
        targetFile.setStatus(TsFileResourceStatus.NORMAL);
      }

    } catch (Exception e) {

    } finally {
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        printLogWhenException(LOGGER, e);
      }
    }

    return isSuccess;
  }

  private File generateTargetFile() throws IOException {
    String path = unseqFileToInsert.getTsFile().getParentFile().getAbsolutePath();
    path = path.replace("unsequence", "sequence");
    TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(unseqFileToInsert.getTsFile().getName());
    tsFileName.setTime(timestamp);
    String fileNameStr = String.format("%d-%d-%d-%d.tsfile", tsFileName.getTime(), tsFileName.getVersion(), tsFileName.getInnerCompactionCnt(), 0);
    return new File(path + File.separator + fileNameStr);
  }

  private void prepareTargetFiles() throws IOException {
    File sourceTsFile = unseqFileToInsert.getTsFile();
    File targetTsFile = targetFile.getTsFile();
    Files.createLink(targetTsFile.toPath(), sourceTsFile.toPath());
    Files.createLink(
        new File(targetTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
        new File(sourceTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
    if (unseqFileToInsert.getModFile().exists()) {
      Files.createLink(
          new File(targetTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath(),
          new File(sourceTsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).toPath());
    }
  }

  @Override
  protected void recover() {

  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InsertionCrossSpaceCompactionTask)) {
      return false;
    }
    return false;
  }

  @Override
  public long getEstimatedMemoryCost() {
    return 0;
  }

  @Override
  public int getProcessedFileNum() {
    return 0;
  }

  @Override
  protected void createSummary() {}
}
