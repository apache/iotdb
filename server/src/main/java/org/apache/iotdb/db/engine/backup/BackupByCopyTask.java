package org.apache.iotdb.db.engine.backup;

import org.apache.iotdb.db.concurrent.WrappedRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BackupByCopyTask extends WrappedRunnable {
  private static final Logger logger = LoggerFactory.getLogger(BackupByCopyTask.class);
  String sourcePath;
  String targetPath;

  public BackupByCopyTask(String sourcePath, String targetPath) {
    this.sourcePath = sourcePath;
    this.targetPath = targetPath;
  }

  @Override
  public void runMayThrow() throws Exception {
    backupByCopy();
  }

  public void backupByCopy() {
    try {
      Files.copy(Paths.get(sourcePath), Paths.get(targetPath));
    } catch (IOException e) {
      logger.error(
          String.format("Copy failed during backup: from %s to %s", sourcePath, targetPath));
    }
  }
}
