package org.apache.iotdb.db.engine.backup.executor;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.backup.task.BackupByCopyTask;
import org.apache.iotdb.db.engine.backup.task.BackupByMoveTask;
import org.apache.iotdb.db.utils.BackupUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFullBackupExecutor extends AbstractBackupExecutor {

  @Override
  public boolean checkBackupPathValid(String outputPath) {
    File tempFile = new File(outputPath);
    try {
      if (!tempFile.createNewFile()) {
        if (tempFile.isFile()) {
          // Full backup output path can not be an existing file.
          return false;
        }
        String[] files = tempFile.list();
        if (files != null && files.length != 0) {
          // throw new WriteProcessException("Can not perform full backup to a non-empty folder.");
          return false;
        }
      } else {
        tempFile.delete();
      }
    } catch (IOException e) {
      // throw new WriteProcessException("Failed to create backup output directory.");
      return false;
    }
    return true;
  }

  @Override
  protected int backupSystemFiles(String outputPath) throws IOException {
    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    List<File> systemFiles = BackupUtils.getAllFilesInOneDir(systemDirPath);
    for (File file : systemFiles) {
      String systemFileTargetPath = BackupUtils.getSystemFileTargetPath(file, outputPath);
      // logger.error("Failed to create directory during backup: " + e.getMessage());
      if (!BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTargetPath), file)) {
        String systemFileTmpPath = BackupUtils.getSystemFileTmpLinkPath(file);
        BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTmpPath), file);
        backupFileTaskList.add(new BackupByMoveTask(systemFileTmpPath, systemFileTargetPath));
      }
    }
    return systemFiles.size();
  }

  @Override
  protected int backupConfigFiles(String outputPath) throws IOException {
    String configDirPath = BackupUtils.getConfDir();
    List<File> configFiles = new ArrayList<>();
    if (configDirPath != null) {
      configFiles = BackupUtils.getAllFilesInOneDir(configDirPath);
      for (File file : configFiles) {
        String configFileTargetPath = BackupUtils.getConfigFileTargetPath(file, outputPath);
        // logger.error("Failed to create directory during backup: " + e.getMessage());
        if (!BackupUtils.createTargetDirAndTryCreateLink(new File(configFileTargetPath), file)) {
          backupFileTaskList.add(
              new BackupByCopyTask(file.getAbsolutePath(), configFileTargetPath));
        }
      }
    } else {
      // logger.warn("Can't find config directory during backup, skipping.");
    }
    return configFiles.size();
  }
}
