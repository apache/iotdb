package org.apache.iotdb.db.engine.backup.executor;

import org.apache.iotdb.db.engine.backup.task.AbstractBackupFileTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.BackupService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBackupExecutor {
  /** Records the files that can't be hard-linked and should be copied. */
  protected List<AbstractBackupFileTask> backupFileTaskList = new ArrayList<>();

  protected BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack;
  protected BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack;

  protected AbstractBackupExecutor(
      BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    this.onSubmitBackupTaskCallBack = onSubmitBackupTaskCallBack;
    this.onBackupFileTaskFinishCallBack = onBackupFileTaskFinishCallBack;
  }

  public abstract boolean checkBackupPathValid(String outputPath);

  public abstract void executeBackup(
      List<TsFileResource> resources, String outputPath, boolean isSync);

  protected abstract int backupSystemFiles(String outputPath) throws IOException;

  protected abstract int backupConfigFiles(String outputPath) throws IOException;
}
