package org.apache.iotdb.db.engine.backup.task;

import org.apache.iotdb.db.service.BackupService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyTask extends AbstractBackupFileTask {
  private static final Logger logger = LoggerFactory.getLogger(DummyTask.class);

  public DummyTask(String sourcePath, String targetPath) {
    super(sourcePath, targetPath);
  }

  @Override
  public boolean backupFile() {
    if (BackupService.getINSTANCE().getBackupByCopyCount().addAndGet(-1) == 0) {
      logger.info("Backup completed.");
      BackupService.getINSTANCE().cleanUpBackupTmpDir();
      BackupService.getINSTANCE().getIsBackupRunning().set(false);
    }
    return true;
  }
}
