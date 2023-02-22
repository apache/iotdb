package org.apache.iotdb.db.engine.backup;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class BackupTsFileTask extends WrappedRunnable {
  private static final Logger logger = LoggerFactory.getLogger(BackupTsFileTask.class);
  TsFileResource resource;
  String outputRootPath;

  public BackupTsFileTask(TsFileResource resource, String outputRootPath) {
    this.resource = resource;
    this.outputRootPath = outputRootPath;
  }

  @Override
  public void runMayThrow() throws Exception {
    backupTsFile();
  }

  public void backupTsFile() {
    try {
      String outputFilePath =
          FilePathUtils.regularizePath(outputRootPath)
              + "data"
              + File.separator
              + resource.getTsFile().getAbsolutePath().replaceFirst(":", "");
      createHardLink(new File(outputFilePath), resource.getTsFile());
      createHardLink(
          new File(outputFilePath + TsFileResource.RESOURCE_SUFFIX),
          new File(resource.getTsFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      if (resource.getModFile().exists()) {
        createHardLink(
            new File(outputFilePath + ModificationFile.FILE_SUFFIX),
            new File(resource.getTsFile().getAbsolutePath() + ModificationFile.FILE_SUFFIX));
      }
      resource.readUnlock();
    } catch (IOException e) {
      logger.error("Illegal TsFile path during backup: " + e.getMessage());
    }
  }

  public static void createHardLink(File target, File source) throws IOException {
    File targetParent = new File(target.getParent());
    if (!targetParent.exists() && !targetParent.mkdirs()) {
      throw new IOException("Cannot create directory " + targetParent.getAbsolutePath());
    }
    Files.deleteIfExists(target.toPath());
    Files.createLink(target.toPath(), source.toPath());
  }
}
