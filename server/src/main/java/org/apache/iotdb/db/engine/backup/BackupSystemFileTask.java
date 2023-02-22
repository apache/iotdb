package org.apache.iotdb.db.engine.backup;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupSystemFileTask extends WrappedRunnable {
  private static final Logger logger = LoggerFactory.getLogger(BackupSystemFileTask.class);
  File sourceFile;
  String outputRootPath;

  public BackupSystemFileTask(File sourceFile, String outputRootPath) {
    this.sourceFile = sourceFile;
    this.outputRootPath = outputRootPath;
  }

  @Override
  public void runMayThrow() throws Exception {}

  public void backupSystemFile() {
    try {
      String systemPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
      File systemDir = new File(systemPath);
      if (sourceFile.getAbsolutePath().contains(systemDir.getAbsolutePath())) {
        String relativeSourcePath = sourceFile.getAbsolutePath().replace(systemDir.getAbsolutePath(), "");
        String outputFilePath =
            FilePathUtils.regularizePath(outputRootPath)
                + IoTDBConstant.SYSTEM_FOLDER_NAME
                + relativeSourcePath;
        BackupTsFileTask.createHardLink(new File(outputFilePath), sourceFile);
      } else {
        throw new IOException(sourceFile.getAbsolutePath());
      }
    } catch (IOException e) {
      logger.error("Illegal System File path during backup: " + e.getMessage());
    }
  }
}
