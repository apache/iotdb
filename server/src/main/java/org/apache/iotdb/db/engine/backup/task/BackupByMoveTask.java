package org.apache.iotdb.db.engine.backup.task;

import org.apache.iotdb.db.service.BackupService;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupByMoveTask extends AbstractBackupFileTask {
  private static final Logger logger = LoggerFactory.getLogger(BackupByMoveTask.class);

  public BackupByMoveTask(String sourcePath, String targetPath) {
    super(sourcePath, targetPath);
  }

  @Override
  public boolean backupFile() {
    boolean isSuccess = true;
    try {
      logger.info(String.format("Moving temporary file: from %s to %s", sourcePath, targetPath));
      FileUtils.moveFile(new File(sourcePath), new File(targetPath));
    } catch (IOException e) {
      isSuccess = false;
      logger.error(
          String.format(
              "Failed to move temporary file during backup: from %s to %s",
              sourcePath, targetPath));
    }
    if (BackupService.getINSTANCE().getBackupByCopyCount().addAndGet(-1) == 0) {
      logger.info("Backup completed.");
      BackupService.getINSTANCE().cleanUpBackupTmpDir();
      BackupService.getINSTANCE().getIsBackupRunning().set(false);
    }
    return isSuccess;
  }
}
