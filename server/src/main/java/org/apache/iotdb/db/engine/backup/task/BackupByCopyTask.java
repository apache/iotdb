package org.apache.iotdb.db.engine.backup.task;

import org.apache.iotdb.db.service.BackupService;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupByCopyTask extends AbstractBackupFileTask {
  private static final Logger logger = LoggerFactory.getLogger(BackupByCopyTask.class);

  public BackupByCopyTask(
      String sourcePath,
      String targetPath,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(sourcePath, targetPath, onBackupFileTaskFinishCallBack);
  }

  @Override
  public boolean backupFile() {
    boolean isSuccess = true;
    try {
      logger.info(String.format("Copying file: from %s to %s", sourcePath, targetPath));
      FileUtils.copyFile(new File(sourcePath), new File(targetPath));
    } catch (IOException e) {
      isSuccess = false;
      logger.error(
          String.format(
              "Failed to copy temporary file during backup: from %s to %s",
              sourcePath, targetPath));
    }
    onBackupFileTaskFinishCallBack.call();
    return isSuccess;
  }
}
