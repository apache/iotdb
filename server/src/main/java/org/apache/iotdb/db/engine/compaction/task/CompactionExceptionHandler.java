package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompactionExceptionHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  public static void handleException(
      String fullStorageGroupName,
      File logFile,
      List<TsFileResource> targetResourceList,
      List<TsFileResource> seqResourceList,
      List<TsFileResource> unseqResourceList,
      TsFileManager tsFileManager,
      long timePartition,
      boolean isInnerSpace,
      boolean isTargetSequence) {
    String compactionType = isInnerSpace ? "inner" : "cross";
    try {
      if (logFile == null || !logFile.exists()) {
        // the log file is null or the log file does not exists
        // it means that compaction has not started yet
        // we don't need to handle it
        return;
      }
      LOGGER.info(
          "{} [Compaction][ExceptionHandler] {} space compaction start handling exception, source seqFiles is {}, source unseqFiles is {}.",
          fullStorageGroupName,
          compactionType,
          seqResourceList,
          unseqResourceList);

      boolean handleSuccess = true;

      List<TsFileResource> lostSourceFiles = new ArrayList<>();

      boolean allSourceSeqFilesExist = checkAllSourceFileExists(seqResourceList, lostSourceFiles);
      boolean allSourceUnseqFilesExist =
          checkAllSourceFileExists(unseqResourceList, lostSourceFiles);

      if (allSourceSeqFilesExist && allSourceUnseqFilesExist) {
        handleSuccess =
            handleWhenAllSourceFilesExist(
                targetResourceList,
                seqResourceList,
                unseqResourceList,
                tsFileManager,
                timePartition,
                isTargetSequence,
                fullStorageGroupName);
      } else {
        handleSuccess =
            handleWhenSomeSourceFilesLost(
                targetResourceList,
                seqResourceList,
                unseqResourceList,
                lostSourceFiles,
                fullStorageGroupName);
      }

      if (!handleSuccess) {
        LOGGER.error(
            "[Compaction][ExceptionHandler] Fail to handle {} space compaction exception, set allowCompaction to false in {}",
            compactionType,
            fullStorageGroupName);
        tsFileManager.setAllowCompaction(false);
      } else {
        FileUtils.delete(logFile);
      }
    } catch (Throwable throwable) {
      // catch throwable when handling exception
      // set the allowCompaction to false
      LOGGER.error(
          "[Compaction][ExceptionHandler] exception occurs when handling exception in {} space compaction. Set allowCompaction to false in {}",
          compactionType,
          fullStorageGroupName,
          throwable);
      tsFileManager.setAllowCompaction(false);
    }
  }

  private static boolean checkAllSourceFileExists(
      List<TsFileResource> tsFileResources, List<TsFileResource> lostFiles) {
    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.getTsFile().exists() || !tsFileResource.resourceFileExists()) {
        lostFiles.add(tsFileResource);
      }
    }
    return lostFiles.size() == 0;
  }

  /**
   * When all source files exists: (1) delete compaction mods files (2) delete target files, tmp
   * target files and its corresponding files (3) recover memory. To avoid triggering OOM again
   * under OOM errors, we do not check whether the target files are complete.
   */
  private static boolean handleWhenAllSourceFilesExist(
      List<TsFileResource> targetResourceList,
      List<TsFileResource> sourceSeqResourceList,
      List<TsFileResource> sourceUnseqResourceList,
      TsFileManager tsFileManager,
      long timePartition,
      boolean isTargetSequence,
      String fullStorageGroupName)
      throws IOException {
    TsFileResourceList unseqTsFileResourceList =
        tsFileManager.getUnsequenceListByTimePartition(timePartition);
    TsFileResourceList seqTsFileResourceList =
        tsFileManager.getSequenceListByTimePartition(timePartition);

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(sourceSeqResourceList, sourceUnseqResourceList);

    boolean removeAllTargetFile = true;
    tsFileManager.writeLock("CompactionExceptionHandler");
    try {
      for (TsFileResource targetTsFile : targetResourceList) {
        // delete target file
        targetTsFile.writeLock();
        if (!targetTsFile.remove()) {
          LOGGER.error(
              "{} [Compaction][Exception] fail to delete target tsfile {} when handling exception",
              fullStorageGroupName,
              targetTsFile);
          removeAllTargetFile = false;
        }
        targetTsFile.writeUnlock();

        // remove target tsfile resource in memory
        if (targetTsFile.isFileInList()) {
          if (isTargetSequence) {
            seqTsFileResourceList.remove(targetTsFile);
          } else {
            unseqTsFileResourceList.remove(targetTsFile);
          }
          TsFileResourceManager.getInstance().removeTsFileResource(targetTsFile);
        }
      }

      // recover source tsfile resource in memory
      for (TsFileResource tsFileResource : sourceSeqResourceList) {
        if (!tsFileResource.isFileInList()) {
          seqTsFileResourceList.keepOrderInsert(tsFileResource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
        }
      }
      for (TsFileResource tsFileResource : sourceUnseqResourceList) {
        if (!tsFileResource.isFileInList()) {
          unseqTsFileResourceList.keepOrderInsert(tsFileResource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
        }
      }
    } finally {
      tsFileManager.writeUnlock();
    }
    return removeAllTargetFile;
  }

  /**
   * Some source files are lost, check if all target files are complete. If all target files are
   * complete, delete the remaining source files and compaction mods files. If some target files are
   * not complete, set the allowCompaction in tsFileManager to false and print some error logs.
   */
  private static boolean handleWhenSomeSourceFilesLost(
      List<TsFileResource> targetResourceList,
      List<TsFileResource> sourceSeqResourceList,
      List<TsFileResource> sourceUnseqResourceList,
      List<TsFileResource> lostSourceResourceList,
      String fullStorageGroupName)
      throws IOException {
    // check whether is all target files complete
    if (!checkIsTargetFilesComplete(
        targetResourceList, lostSourceResourceList, fullStorageGroupName)) {
      return false;
    }

    // delete source files
    for (TsFileResource resource : sourceSeqResourceList) {
      resource.setDeleted(true);
      resource.remove();
    }
    for (TsFileResource resource : sourceUnseqResourceList) {
      resource.setDeleted(true);
      resource.remove();
    }

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(sourceSeqResourceList, sourceUnseqResourceList);

    return true;
  }

  private static boolean checkIsTargetFilesComplete(
      List<TsFileResource> targetResources,
      List<TsFileResource> lostSourceResources,
      String fullStorageGroupName)
      throws IOException {
    for (TsFileResource targetResource : targetResources) {
      if (!TsFileUtils.isTsFileComplete(targetResource.getTsFile())) {
        LOGGER.error(
            "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files {} is lost, do nothing. Set allowCompaction to false",
            fullStorageGroupName,
            targetResource,
            lostSourceResources);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        return false;
      }
    }
    return true;
  }
}
