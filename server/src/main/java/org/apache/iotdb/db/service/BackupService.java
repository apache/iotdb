package org.apache.iotdb.db.service;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.backup.BackupSystemFileTask;
import org.apache.iotdb.db.engine.backup.BackupTsFileTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.WriteProcessException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BackupService implements IService {
  private ExecutorService backupThreadPool;

  public static BackupService getINSTANCE() {
    return BackupService.InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final BackupService INSTANCE = new BackupService();

    private InstanceHolder() {}
  }

  @Override
  public void start() throws StartupException {
    if (backupThreadPool == null) {
      int backupThreadNum = IoTDBDescriptor.getInstance().getConfig().getBackupThreadNum();
      backupThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              backupThreadNum, ThreadName.BACKUP_SERVICE.getName());
    }
  }

  @Override
  public void stop() {}

  private void submitBackupTsFileTask(BackupTsFileTask task) {
    task.backupTsFile();
  }

  private void submitBackupSystemFileTask(BackupSystemFileTask task) {
    task.backupSystemFile();
  }

  public void backupFiles(List<TsFileResource> resources, String outputPath)
      throws WriteProcessException {
    File tempFile = new File(outputPath);
    try {
      if (!tempFile.createNewFile()) {
        if (tempFile.isFile()) {
          throw new WriteProcessException("Backup output path can not be an existing file.");
        }
        String[] files = tempFile.list();
        if (files != null && files.length != 0) {
          throw new WriteProcessException("Can not backup into a non-empty folder.");
        }
      } else {
        tempFile.delete();
      }
    } catch (IOException e) {
      throw new WriteProcessException("Failed to create directory for backup.");
    }
    for (TsFileResource resource : resources) {
      submitBackupTsFileTask(new BackupTsFileTask(resource, outputPath));
    }
    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    for (File file : getAllFilesInOneDir(systemDirPath)) {
      submitBackupSystemFileTask(new BackupSystemFileTask(file, outputPath));
    }
  }

  private static List<File> getAllFilesInOneDir(String path) {
    List<File> sonFiles = new ArrayList<>();
    File[] sonFileAndDirs = new File(path).listFiles();
    if (sonFileAndDirs != null) {
      for (File f : sonFileAndDirs) {
        if (f.isFile()) {
          sonFiles.add(f);
        } else {
          sonFiles.addAll(getAllFilesInOneDir(f.getAbsolutePath()));
        }
      }
    }
    return sonFiles;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.BACKUP_SERVICE;
  }
}
