package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompactionExceptionHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  public static void handleException(
      String storageGroup,
      File logFile,
      List<TsFileResource> targetResourceList,
      List<TsFileResource> sourceResourceList,
      TsFileManager tsFileManager,
      long timePartition,
      boolean isInnerSpace) {
    String compactionType = isInnerSpace ? "inner" : "cross";
    try {
      if (logFile == null || !logFile.exists()) {
        // the log file is null or the log file does not exists
        // it means that compaction has not started yet
        // we don't need to handle it
        return;
      }
      LOGGER.info(
          "{} [Compaction][ExceptionHandler] {} space compaction start handling exception, source files is {}",
          storageGroup,
          compactionType,
          sourceResourceList);

      boolean handleSuccess = true;

      List<TsFileResource> lostSourceFiles = new ArrayList<>();

      boolean allSourceFilesExist = checkAllSourceFileExists(sourceResourceList, lostSourceFiles);

      if (allSourceFilesExist) {
        handleSuccess =
            handleWhenAllSourceFilesExist(
                storageGroup, targetResourceList, sourceResourceList, tsFileManager, timePartition);
      } else {
        handleSuccess =
            handleWhenSomeSourceFilesLost(
                storageGroup, seqResourceList, unseqResourceList, logFile, isInnerSpace);
      }

      if (!handleSuccess) {
        LOGGER.error(
            "[Compaction][ExceptionHandler] Fail to handle {} space compaction exception, set allowCompaction to false in {}",
            compactionType,
            storageGroup);
        tsFileManager.setAllowCompaction(false);
      } else {
        FileUtils.delete(logFile);
      }
    } catch (Throwable throwable) {
      // catch throwable when handling exception
      // set the allowCompaction to false
      LOGGER.error(
          "[Compaction][ExceptionHandler] exception occurs when handling exception in {} space compaction."
              + " Set allowCompaction to false in {}",
          compactionType,
          storageGroup,
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
      String storageGroup,
      List<TsFileResource> targetTsFiles,
      List<TsFileResource> sourceResourceList,
      TsFileManager tsFileManager,
      long timePartition)
      throws IOException {
    TsFileResourceList unseqTsFileResourceList =
        tsFileManager.getUnsequenceListByTimePartition(timePartition);
    TsFileResourceList seqTsFileResourceList =
        tsFileManager.getSequenceListByTimePartition(timePartition);

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(sourceResourceList, Collections.emptyList());

    boolean removeAllTargetFile = true;
    tsFileManager.writeLock("CompactionExceptionHandler");
    try {
      for (TsFileResource targetTsFile : targetTsFiles) {
        // delete target files
        targetTsFile.writeLock();
        if (!targetTsFile.remove()) {
          LOGGER.error(
              "{} [Compaction][Exception] fail to delete target tsfile {} when handling exception",
              storageGroup,
              targetTsFile);
          removeAllTargetFile = false;
        }
        targetTsFile.writeUnlock();

        // remove target tsfile resource in memory
        if (targetTsFile.isFileInList()) {
          seqTsFileResourceList.remove(targetTsFile);
          TsFileResourceManager.getInstance().removeTsFileResource(targetTsFile);
        }
      }

      // recover source tsfile resource in memory
      for (TsFileResource tsFileResource : sourceResourceList) {
        if (!tsFileResource.isFileInList()) {
          seqTsFileResourceList.keepOrderInsert(tsFileResource);
          TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
        }
      }
    } finally {
      tsFileManager.writeUnlock();
    }
    return removeAllTargetFile;
  }

  /**
   * Some source files are lost, check if the compaction has finished. If the compaction has
   * finished, delete the remaining source files and compaction mods files. If the compaction has
   * not finished, set the allowCompaction in tsFileManager to false and print some error logs.
   */
  public static boolean handleWhenSomeSourceFilesLost(
      String storageGroup,
      List<TsFileResource> sourceResourceList,
      File logFile,
      boolean isInnerSpace)
      throws IOException {
    if (isInnerSpace) {

    } else {

    }

    // delete source files
    for (TsFileResource resource : sourceResourceList) {
      resource.remove();
      resource.setDeleted(true);
    }

    // delete compaction mods files
    CompactionUtils.deleteCompactionModsFile(sourceResourceList, Collections.emptyList());

    return true;
  }

  private static boolean checkIsInnerSpaceCompactionCorrect(
      TsFileResource targetTsFile,
      List<TsFileResource> lostSourceFiles,
      String fullStorageGroupName)
      throws IOException {
    if (!TsFileUtils.isTsFileComplete(targetTsFile.getTsFile())) {
      // target file is not complete, and some source file is lost
      // some data is lost
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] target file {} is not complete, and some source files {} is lost, do nothing. Set allowCompaction to false",
          fullStorageGroupName,
          targetTsFile,
          lostSourceFiles);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      return false;
    }
    return true;
  }

  private static boolean checkIsCrossSpaceCompactionCorrect(
      File logFile, String fullStorageGroupName) throws IOException {
    long magicStringLength =
        RewriteCrossSpaceCompactionLogger.MAGIC_STRING.getBytes(StandardCharsets.UTF_8).length;
    long fileLength = logFile.length();

    if (fileLength < 2 * magicStringLength) {
      // the log length is less than twice the Magic String length
      // it means the compaction has not finished yet
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the compaction log length is less than twice "
              + "the MagicString length",
          fullStorageGroupName);
      return false;
    }

    // read head magic string in compaction log
    RewriteCrossSpaceCompactionLogAnalyzer logAnalyzer =
        new RewriteCrossSpaceCompactionLogAnalyzer(logFile);
    logAnalyzer.analyze();
    if (!logAnalyzer.isFirstMagicStringExisted()) {
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the head magic string in compaction log is incorrect,"
              + " failed to handle exception",
          fullStorageGroupName);
      return false;
    }

    // read tail string in compaction log
    if (!logAnalyzer.isEndMagicStringExisted()) {
      LOGGER.error(
          "{} [Compaction][ExceptionHandler] the tail magic string in compaction log is incorrect,"
              + " failed to handle exception",
          fullStorageGroupName);
      return false;
    }

    return true;
  }
}
