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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RewriteCrossCompactionRecoverTask extends RewriteCrossSpaceCompactionTask {
  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private File compactionLogFile;

  public RewriteCrossCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      File logFile,
      AtomicInteger currentTaskNum,
      TsFileManager tsFileManager) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        tsFileManager,
        null,
        null,
        currentTaskNum);
    this.compactionLogFile = logFile;
  }

  @Override
  public void doCompaction() {
    boolean handleSuccess = true;
    LOGGER.info(
        "{} [Compaction][Recover] cross space compaction log is {}",
        fullStorageGroupName,
        compactionLogFile);
    try {
      if (compactionLogFile.exists()) {
        LOGGER.info(
            "{} [Compaction][Recover] cross space compaction log file {} exists, start to recover it",
            fullStorageGroupName,
            compactionLogFile);
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(compactionLogFile);
        if (compactionLogFile.getName().equals(CompactionLogger.COMPACTION_LOG_NAME_FEOM_OLD)) {
          // log from previous version (<0.13)
          logAnalyzer.analyzeOldCrossCompactionLog();
        } else {
          logAnalyzer.analyze();
        }
        List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
        List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();

        // compaction log file is incomplete
        if (targetFileIdentifiers.isEmpty() || sourceFileIdentifiers.isEmpty()) {
          LOGGER.info(
              "{} [Compaction][Recover] incomplete log file, abort recover", fullStorageGroupName);
          return;
        }

        // check is all source files existed
        boolean isAllSourcesFileExisted = true;
        for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
          File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
          if (sourceFile == null) {
            isAllSourcesFileExisted = false;
            break;
          }
        }
        if (isAllSourcesFileExisted) {
          if (logAnalyzer.isLogFromOld()) {
            handleSuccess = handleWithAllSourceFilesExistFromOld(targetFileIdentifiers);
          } else {
            handleSuccess =
                handleWithAllSourceFilesExist(targetFileIdentifiers, sourceFileIdentifiers);
          }
        } else {
          if (logAnalyzer.isLogFromOld()) {
            handleSuccess =
                handleWithoutAllSourceFilesExistFromOld(
                    targetFileIdentifiers, sourceFileIdentifiers);
          } else {
            handleSuccess = handleWithoutAllSourceFilesExist(sourceFileIdentifiers);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("recover cross space compaction error", e);
    } finally {
      if (!handleSuccess) {
        LOGGER.error(
            "{} [Compaction][Recover] Failed to recover cross space compaction, set allowCompaction to false",
            fullStorageGroupName);
        tsFileManager.setAllowCompaction(false);
      } else {
        if (compactionLogFile.exists()) {
          try {
            LOGGER.info(
                "{} [Compaction][Recover] Recover compaction successfully, delete log file {}",
                fullStorageGroupName,
                compactionLogFile);
            FileUtils.delete(compactionLogFile);
          } catch (IOException e) {
            LOGGER.error(
                "{} [Compaction][Recover] Exception occurs while deleting log file {}, set allowCompaction to false",
                fullStorageGroupName,
                compactionLogFile,
                e);
            tsFileManager.setAllowCompaction(false);
          }
        }
      }
    }
  }

  /**
   * All source files exist: (1) delete all the target files and tmp target files (2) delete
   * compaction mods files.
   */
  private boolean handleWithAllSourceFilesExist(
      List<TsFileIdentifier> targetFileIdentifiers, List<TsFileIdentifier> sourceFileIdentifiers) {
    LOGGER.info(
        "{} [Compaction][Recover] all source files exists, delete all target files.",
        fullStorageGroupName);

    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      // xxx.cross
      File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
      // xxx.tsfile
      File targetFile =
          getFileFromDataDirs(
              targetFileIdentifier
                  .getFilePath()
                  .replace(
                      IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX,
                      TsFileConstant.TSFILE_SUFFIX));
      TsFileResource targetResource;
      if (tmpTargetFile != null) {
        targetResource = new TsFileResource(tmpTargetFile);
      } else {
        targetResource = new TsFileResource(targetFile);
      }

      if (!targetResource.remove()) {
        // failed to remove tmp target tsfile
        // system should not carry out the subsequent compaction in case of data redundant
        LOGGER.warn(
            "{} [Compaction][Recover] failed to remove target file {}",
            fullStorageGroupName,
            targetResource);
        return false;
      }
    }

    // delete compaction mods files
    List<TsFileResource> sourceTsFileResourceList = new ArrayList<>();
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      sourceTsFileResourceList.add(new TsFileResource(sourceFileIdentifier.getFileFromDataDirs()));
    }
    try {
      CompactionUtils.deleteCompactionModsFile(sourceTsFileResourceList, Collections.emptyList());
    } catch (Throwable e) {
      LOGGER.error(
          "{} [Compaction][Recover] Exception occurs while deleting compaction mods file, set allowCompaction to false",
          fullStorageGroupName,
          e);
      return false;
    }
    return true;
  }

  /**
   * Some source files lost: delete remaining source files, encluding: tsfile, resource file, mods
   * file and compaction mods file.
   */
  private boolean handleWithoutAllSourceFilesExist(List<TsFileIdentifier> sourceFileIdentifiers) {
    // some source files have been deleted, while target file must exist.
    boolean handleSuccess = true;
    List<TsFileResource> remainSourceTsFileResources = new ArrayList<>();
    for (TsFileIdentifier sourceFileIdentifier : sourceFileIdentifiers) {
      File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
      if (sourceFile != null) {
        remainSourceTsFileResources.add(new TsFileResource(sourceFile));
      }
      // delete .compaction.mods file and .mods file of all source files
      File compactionModFile =
          getFileFromDataDirs(
              sourceFileIdentifier.getFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX);
      File modFile =
          getFileFromDataDirs(sourceFileIdentifier.getFilePath() + ModificationFile.FILE_SUFFIX);
      if (compactionModFile != null && !compactionModFile.delete()) {
        LOGGER.error(
            "{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
            fullStorageGroupName,
            compactionModFile);
        handleSuccess = false;
      }
      if (modFile != null && !modFile.delete()) {
        LOGGER.error(
            "{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
            fullStorageGroupName,
            modFile);
        handleSuccess = false;
      }
    }
    // delete remaining source files
    if (!InnerSpaceCompactionUtils.deleteTsFilesInDisk(
        remainSourceTsFileResources, fullStorageGroupName)) {
      LOGGER.error(
          "{} [Compaction][Recover] fail to delete remaining source files.", fullStorageGroupName);
      handleSuccess = false;
    }
    return handleSuccess;
  }

  /** Delete tmp target file and compaction mods file. */
  private boolean handleWithAllSourceFilesExistFromOld(
      List<TsFileIdentifier> targetFileIdentifiers) {
    // delete tmp target file
    for (TsFileIdentifier targetFileIdentifier : targetFileIdentifiers) {
      // xxx.tsfile.merge
      File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
      if (tmpTargetFile != null) {
        tmpTargetFile.delete();
      }
    }
    File compactionModsFileFromOld =
        new File(
            tsFileManager.getStorageGroupDir()
                + File.separator
                + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
    if (compactionModsFileFromOld.exists() && !compactionModsFileFromOld.delete()) {
      LOGGER.error(
          "{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
          fullStorageGroupName,
          compactionModsFileFromOld);
      return false;
    }
    return true;
  }

  /**
   * 1. If target file does not exist, then move .merge file to target file <br>
   * 2. If target resource file does not exist, then serialize it. <br>
   * 3. Append merging modification to target mods file and delete merging mods file. <br>
   * 4. Delete source files and .merge file. <br>
   */
  private boolean handleWithoutAllSourceFilesExistFromOld(
      List<TsFileIdentifier> targetFileIdentifiers, List<TsFileIdentifier> sourceFileIdentifiers) {
    try {
      File compactionModsFileFromOld =
          new File(
              tsFileManager.getStorageGroupDir()
                  + File.separator
                  + IoTDBConstant.COMPACTION_MODIFICATION_FILE_NAME_FROM_OLD);
      List<TsFileResource> targetFileResources = new ArrayList<>();
      for (int i = 0; i < sourceFileIdentifiers.size(); i++) {
        TsFileIdentifier sourceFileIdentifier = sourceFileIdentifiers.get(i);
        if (sourceFileIdentifier.isSequence()) {
          File tmpTargetFile = targetFileIdentifiers.get(i).getFileFromDataDirs();
          File targetFile = null;

          // move tmp target file to target file if not exist
          if (tmpTargetFile != null) {
            // move tmp target file to target file
            String sourceFilePath =
                tmpTargetFile
                    .getPath()
                    .replace(
                        TsFileConstant.TSFILE_SUFFIX
                            + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD,
                        TsFileConstant.TSFILE_SUFFIX);
            targetFile = TsFileNameGenerator.increaseCrossCompactionCnt(new File(sourceFilePath));
            FSFactoryProducer.getFSFactory().moveFile(tmpTargetFile, targetFile);
          } else {
            // target file must exist
            File file =
                TsFileNameGenerator.increaseCrossCompactionCnt(
                    new File(
                        targetFileIdentifiers
                            .get(i)
                            .getFilePath()
                            .replace(
                                TsFileConstant.TSFILE_SUFFIX
                                    + IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX_FROM_OLD,
                                TsFileConstant.TSFILE_SUFFIX)));

            targetFile = getFileFromDataDirs(file.getPath());
          }
          if (targetFile == null) {
            LOGGER.error(
                "{} [Compaction][Recover] target file of source seq file {} does not exist (<0.13).",
                fullStorageGroupName,
                sourceFileIdentifier.getFilePath());
            return false;
          }

          // serialize target resource file if not exist
          TsFileResource targetResource = new TsFileResource(targetFile);
          if (!targetResource.resourceFileExists()) {
            try (TsFileSequenceReader reader =
                new TsFileSequenceReader(targetFile.getAbsolutePath())) {
              FileLoaderUtils.updateTsFileResource(reader, targetResource);
            }
            targetResource.serialize();
          }

          targetFileResources.add(targetResource);

          // append compaction modifications to target mods file and delete compaction mods file
          if (compactionModsFileFromOld.exists()) {
            ModificationFile compactionModsFile =
                new ModificationFile(compactionModsFileFromOld.getPath());
            appendCompactionModificationsFromOld(targetResource, compactionModsFile);
          }

          // delete tmp target file
          if (tmpTargetFile != null) {
            tmpTargetFile.delete();
          }
        }

        // delete source tsfile
        File sourceFile = sourceFileIdentifier.getFileFromDataDirs();
        if (sourceFile != null) {
          sourceFile.delete();
        }

        // delete source resource file
        sourceFile =
            getFileFromDataDirs(
                sourceFileIdentifier.getFilePath() + TsFileResource.RESOURCE_SUFFIX);
        if (sourceFile != null) {
          sourceFile.delete();
        }

        // delete source mods file
        sourceFile =
            getFileFromDataDirs(sourceFileIdentifier.getFilePath() + ModificationFile.FILE_SUFFIX);
        if (sourceFile != null) {
          sourceFile.delete();
        }
      }

      // delete compaction mods file
      if (compactionModsFileFromOld.exists() && !compactionModsFileFromOld.delete()) {
        LOGGER.error(
            "{} [Compaction][Recover] fail to delete target file {}, this may cause data incorrectness",
            fullStorageGroupName,
            compactionModsFileFromOld);
        return false;
      }
    } catch (Throwable e) {
      LOGGER.error(
          "{} [Compaction][Recover] fail to handle with some source files lost from old version.",
          fullStorageGroupName,
          e);
      return false;
    }

    return true;
  }

  public void appendCompactionModificationsFromOld(
      TsFileResource resource, ModificationFile compactionModsFile) throws IOException {

    if (compactionModsFile != null) {
      for (Modification modification : compactionModsFile.getModifications()) {
        // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
        // change after compaction
        modification.setFileOffset(Long.MAX_VALUE);
        resource.getModFile().write(modification);
      }
      resource.getModFile().close();
    }
  }

  /**
   * This method find the File object of given filePath by searching it in every data directory. If
   * the file is not found, it will return null.
   */
  private File getFileFromDataDirs(String filePath) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      File f = new File(dataDir, filePath);
      if (f.exists()) {
        return f;
      }
    }
    return null;
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossCompactionRecoverTask) {
      return compactionLogFile.equals(
          ((RewriteCrossCompactionRecoverTask) other).compactionLogFile);
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return compactionLogFile.exists();
  }
}
