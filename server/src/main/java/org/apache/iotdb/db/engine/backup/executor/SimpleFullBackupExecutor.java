package org.apache.iotdb.db.engine.backup.executor;

import org.apache.iotdb.db.engine.backup.task.BackupByMoveTask;
import org.apache.iotdb.db.engine.backup.task.DummyTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.BackupService;
import org.apache.iotdb.db.utils.BackupUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleFullBackupExecutor extends AbstractFullBackupExecutor {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFullBackupExecutor.class);

  @Override
  public void executeBackup(
      List<TsFileResource> resources,
      String outputPath,
      boolean isSync,
      BackupService.SubmitBackupTaskCallBack submitBackupTaskCallBack) {
    if (!checkBackupPathValid(outputPath)) {
      logger.error("Full backup path invalid. Backup aborted.");
      return;
    }
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.error("Failed to delete backup temporary directories before backup. Backup aborted.");
      return;
    }
    for (TsFileResource resource : resources) {
      try {
        String tsfileTargetPath = BackupUtils.getTsFileTargetPath(resource.getTsFile(), outputPath);
        if (BackupUtils.createTargetDirAndTryCreateLink(
            new File(tsfileTargetPath), resource.getTsFile())) {
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupUtils.createTargetDirAndTryCreateLink(
                new File(tsfileTargetPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
          }
        } else {
          String tsfileTmpPath = BackupUtils.getTsFileTmpLinkPath(resource.getTsFile());
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath), resource.getTsFile());
          backupFileTaskList.add(new BackupByMoveTask(tsfileTmpPath, tsfileTargetPath));
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          backupFileTaskList.add(
              new BackupByMoveTask(
                  tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX,
                  tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupUtils.createTargetDirAndTryCreateLink(
                new File(tsfileTmpPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
            backupFileTaskList.add(
                new BackupByMoveTask(
                    tsfileTmpPath + ModificationFile.FILE_SUFFIX,
                    tsfileTargetPath + ModificationFile.FILE_SUFFIX));
          }
        }
      } catch (IOException e) {
        logger.error("Failed to create directory during backup: " + e.getMessage());
      } finally {
        resource.readUnlock();
      }
    }

    int systemFileCount = -1;
    try {
      systemFileCount = backupSystemFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }
    int configFileCount = -1;
    try {
      configFileCount = backupConfigFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }

    logger.info(
        String.format(
            "Backup starting, found %d TsFiles and their related files, %d system files and %d config files.",
            resources.size(), systemFileCount, configFileCount));
    logger.info(
        String.format(
            "%d files can't be hard-linked and should be copied.", backupFileTaskList.size()));

    if (backupFileTaskList.size() == 0) {
      backupFileTaskList.add(new DummyTask("", ""));
    }
    List<Future<Boolean>> taskFutureList = submitBackupTaskCallBack.call(backupFileTaskList);
    if (isSync) {
      boolean isAllSuccess = true;
      try {
        for (Future<Boolean> future : taskFutureList) {
          isAllSuccess = isAllSuccess && future.get();
        }
        // TODO: how to return this status
      } catch (ExecutionException e) {
        // TODO: what's this
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
