package org.apache.iotdb.db.engine.backup.executor;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.backup.task.BackupByCopyTask;
import org.apache.iotdb.db.engine.backup.task.BackupByMoveTask;
import org.apache.iotdb.db.service.BackupService;
import org.apache.iotdb.db.utils.BackupUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractIncrementalBackupExecutor extends AbstractBackupExecutor {

  protected AbstractIncrementalBackupExecutor(
      BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(onSubmitBackupTaskCallBack, onBackupFileTaskFinishCallBack);
  }

  @Override
  public boolean checkBackupPathValid(String outputPath) {
    File tempFile = new File(outputPath);
    String[] files = tempFile.list();
    // Output path for incremental backup should be a non-empty folder.
    return tempFile.exists() && !tempFile.isFile() && files != null;
  }

  @Override
  protected int backupSystemFiles(String outputPath) throws IOException {
    try {
      BackupUtils.deleteOldSystemFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }
    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    List<File> systemFiles = BackupUtils.getAllFilesInOneDir(systemDirPath);
    for (File file : systemFiles) {
      String systemFileTargetPath = BackupUtils.getSystemFileTargetPath(file, outputPath);
      // logger.error("Failed to create directory during backup: " + e.getMessage());
      if (!BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTargetPath), file)) {
        String systemFileTmpPath = BackupUtils.getSystemFileTmpLinkPath(file);
        BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTmpPath), file);
        backupFileTaskList.add(
            new BackupByMoveTask(
                systemFileTmpPath, systemFileTargetPath, onBackupFileTaskFinishCallBack));
      }
    }
    return systemFiles.size();
  }

  @Override
  protected int backupConfigFiles(String outputPath) throws IOException {
    try {
      BackupUtils.deleteOldConfigFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }
    String configDirPath = BackupUtils.getConfDir();
    List<File> configFiles = new ArrayList<>();
    if (configDirPath != null) {
      configFiles = BackupUtils.getAllFilesInOneDir(configDirPath);
      for (File file : configFiles) {
        String configFileTargetPath = BackupUtils.getConfigFileTargetPath(file, outputPath);
        // logger.error("Failed to create directory during backup: " + e.getMessage());
        if (!BackupUtils.createTargetDirAndTryCreateLink(new File(configFileTargetPath), file)) {
          backupFileTaskList.add(
              new BackupByCopyTask(
                  file.getAbsolutePath(), configFileTargetPath, onBackupFileTaskFinishCallBack));
        }
      }
    } else {
      // logger.warn("Can't find config directory during backup, skipping.");
    }
    return configFiles.size();
  }
}
