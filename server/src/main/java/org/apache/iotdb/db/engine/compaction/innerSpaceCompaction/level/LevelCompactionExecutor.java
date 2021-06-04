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

package org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.level;

import static org.apache.iotdb.db.engine.compaction.TsFileManagement.getMergeLevel;
import static org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogger.TARGET_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.InnerSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list */
public class LevelCompactionExecutor extends InnerSpaceCompactionExecutor {

  private static final Logger logger = LoggerFactory.getLogger(LevelCompactionExecutor.class);

  public LevelCompactionExecutor(TsFileManagement tsFileManagement) {
    super(tsFileManagement);
  }

  public void renameLevelFilesMods(
      Collection<Modification> filterModification,
      Collection<TsFileResource> mergeTsFiles,
      TsFileResource targetTsFile)
      throws IOException {
    logger.debug(
        "{} [compaction] merge starts to rename real file's mod",
        tsFileManagement.storageGroupName);
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceModificationFile =
          new ModificationFile(mergeTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        modifications.addAll(sourceModificationFile.getModifications());
        if (sourceModificationFile.exists()) {
          sourceModificationFile.remove();
        }
      }
    }
    modifications.removeAll(filterModification);
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile =
          new ModificationFile(targetTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  /** recover files */
  @Override
  @SuppressWarnings({"squid:S3776", "squid:S2142"})
  public void recover() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                tsFileManagement.storageGroupSysDir,
                tsFileManagement.storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        String targetFile = logAnalyzer.getTargetFile();
        boolean isSeq = logAnalyzer.isSeq();
        if (targetFile == null || sourceFileList.isEmpty()) {
          return;
        }
        File target = new File(targetFile);
        if (deviceSet.isEmpty()) {
          // if not in compaction, just delete the target file
          if (target.exists()) {
            Files.delete(target.toPath());
          }
          return;
        }
        // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
        TsFileResource targetResource =
            tsFileManagement.getRecoverTsFileResource(targetFile, isSeq);
        long timePartition = targetResource.getTimePartition();
        List<TsFileResource> sourceTsFileResources = new ArrayList<>();
        for (String file : sourceFileList) {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          sourceTsFileResources.add(tsFileManagement.getTsFileResource(file, isSeq));
        }
        int level = getMergeLevel(new File(sourceFileList.get(0)));
        RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
        // if not complete compaction, resume merge
        if (writer.hasCrashed()) {
          if (offset > 0) {
            writer.getIOWriterOut().truncate(offset - 1);
          }
          writer.close();
          CompactionLogger compactionLogger =
              new CompactionLogger(
                  tsFileManagement.storageGroupSysDir, tsFileManagement.storageGroupName);
          List<Modification> modifications = new ArrayList<>();
          CompactionUtils.merge(
              targetResource,
              sourceTsFileResources,
              tsFileManagement.storageGroupName,
              compactionLogger,
              deviceSet,
              isSeq,
              modifications);
          // complete compaction and delete source file
          tsFileManagement.writeLock();
          try {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException(
                  String.format("%s [Compaction] abort", tsFileManagement.storageGroupName));
            }
            tsFileManagement.add(targetResource, isSeq);
            tsFileManagement.deleteLevelFilesInList(
                timePartition, sourceTsFileResources, level, isSeq);
          } finally {
            tsFileManagement.writeUnlock();
          }
          tsFileManagement.deleteLevelFilesInDisk(sourceTsFileResources);
          renameLevelFilesMods(modifications, sourceTsFileResources, targetResource);
          compactionLogger.close();
        } else {
          writer.close();
        }
      }
    } catch (IOException | IllegalPathException | InterruptedException e) {
      logger.error("recover level tsfile management error ", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete level tsfile management log file error ", e);
        }
      }
    }
  }

  @Override
  public void doInnerSpaceCompaction(boolean sequence, long timePartition) {
    List<List<TsFileResource>> mergeResources =
        tsFileManagement.getClosedTsFileListByTimePartition(true, timePartition);
    int currMaxLevel =
        sequence
            ? IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum()
            : IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    int currMaxFileNumInEachLevel =
        sequence
            ? IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel()
            : IoTDBDescriptor.getInstance().getConfig().getUnseqFileNumInEachLevel();
    // wait until unseq merge has finished
    long startTimeMillis = System.currentTimeMillis();
    CompactionLogger compactionLogger = null;
    try {
      logger.info("{} start to filter compaction condition", tsFileManagement.storageGroupName);
      for (int i = 0; i < currMaxLevel - 1; i++) {
        List<TsFileResource> currLevelTsFileResource = mergeResources.get(i);
        if (currMaxFileNumInEachLevel <= currLevelTsFileResource.size()) {
          // just merge part of the file
          compactionLogger =
              new CompactionLogger(
                  tsFileManagement.storageGroupSysDir, tsFileManagement.storageGroupName);
          // log source file list and target file for recover
          for (TsFileResource mergeResource : mergeResources.get(i)) {
            compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
          }
          File newLevelFile =
              TsFileResource.modifyTsFileNameMergeCnt(mergeResources.get(i).get(0).getTsFile());
          compactionLogger.logSequence(sequence);
          compactionLogger.logFile(TARGET_NAME, newLevelFile);
          List<TsFileResource> toMergeTsFiles =
              mergeResources.get(i).subList(0, currMaxFileNumInEachLevel);
          logger.info(
              "{} [Compaction] merge level-{}'s {} TsFiles to next level",
              tsFileManagement.storageGroupName,
              i,
              toMergeTsFiles.size());
          for (TsFileResource toMergeTsFile : toMergeTsFiles) {
            logger.info(
                "{} [Compaction] start to merge TsFile {}",
                tsFileManagement.storageGroupName,
                toMergeTsFile);
          }

          TsFileResource newResource = new TsFileResource(newLevelFile);
          List<Modification> modifications = new ArrayList<>();
          // merge, read from source files and write to target file
          CompactionUtils.merge(
              newResource,
              toMergeTsFiles,
              tsFileManagement.storageGroupName,
              compactionLogger,
              new HashSet<>(),
              sequence,
              modifications);
          logger.info(
              "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
              tsFileManagement.storageGroupName,
              i,
              toMergeTsFiles.size());
          tsFileManagement.writeLock();
          try {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException(
                  String.format("%s [Compaction] abort", tsFileManagement.storageGroupName));
            }

            tsFileManagement.add(newResource, sequence);
            tsFileManagement.deleteLevelFilesInList(timePartition, toMergeTsFiles, i, sequence);
            if (mergeResources.size() > i + 1) {
              mergeResources.get(i + 1).add(newResource);
            }
          } finally {
            tsFileManagement.writeUnlock();
          }
          tsFileManagement.deleteLevelFilesInDisk(toMergeTsFiles);
          renameLevelFilesMods(modifications, toMergeTsFiles, newResource);
          compactionLogger.close();
          File logFile =
              FSFactoryProducer.getFSFactory()
                  .getFile(
                      tsFileManagement.storageGroupSysDir,
                      tsFileManagement.storageGroupName + COMPACTION_LOG_NAME);
          if (logFile.exists()) {
            Files.delete(logFile.toPath());
          }
          break;
        }
      }
    } catch (Exception e) {
      if (compactionLogger != null) {
        try {
          compactionLogger.close();
        } catch (IOException ioException) {
          logger.error(
              "{} Compaction log close fail",
              tsFileManagement.storageGroupName + COMPACTION_LOG_NAME);
        }
      }
      restoreCompaction();
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info(
          "{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
          tsFileManagement.storageGroupName,
          sequence,
          System.currentTimeMillis() - startTimeMillis);
    }
  }

  /** restore the files back to the status before the compaction task is submitted */
  private void restoreCompaction() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                tsFileManagement.storageGroupSysDir,
                tsFileManagement.storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        String targetFilePath = logAnalyzer.getTargetFile();
        if (targetFilePath != null) {
          File targetFile = new File(targetFilePath);
          if (targetFile.exists()) {
            if (!targetFile.delete()) {
              logger.warn("Delete file {} failed", targetFile);
            }
          }
        }
      }
    } catch (IOException e) {
      logger.error("restore compaction failed", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete compaction log file error ", e);
        }
      }
    }
  }
}
