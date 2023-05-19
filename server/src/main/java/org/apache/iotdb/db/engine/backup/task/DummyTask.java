package org.apache.iotdb.db.engine.backup.task;

import org.apache.iotdb.db.service.BackupService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyTask extends AbstractBackupFileTask {
  private static final Logger logger = LoggerFactory.getLogger(DummyTask.class);

  public DummyTask(
      String sourcePath,
      String targetPath,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(sourcePath, targetPath, onBackupFileTaskFinishCallBack);
  }

  @Override
  public boolean backupFile() {
    onBackupFileTaskFinishCallBack.call();
    return true;
  }
}
