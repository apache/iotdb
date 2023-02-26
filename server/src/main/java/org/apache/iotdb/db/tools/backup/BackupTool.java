/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.tools.backup;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BackupTool {

  /**
   * @param target The hard link file to be created.
   * @param source The file that is linked to.
   * @return Returns true if the hard link is successfully created, false if failed to create.
   * @throws IOException if failed to create the parent directory of target file.
   */
  public static boolean createTargetDirAndTryCreateLink(File target, File source)
      throws IOException {
    File targetParent = new File(target.getParent());
    if (!targetParent.exists() && !targetParent.mkdirs()) {
      throw new IOException("Cannot create directory " + targetParent.getAbsolutePath());
    }
    Files.deleteIfExists(target.toPath());
    try {
      Files.createLink(target.toPath(), source.toPath());
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  public static String getTsFileTargetPath(TsFileResource resource, String outputBaseDir) {
    return FilePathUtils.regularizePath(outputBaseDir)
        + "data"
        + File.separator
        + resource.getTsFile().getAbsolutePath().replaceFirst(":", "");
  }

  public static String getSystemFileTargetPath(File source, String outputBaseDir) {
    String systemPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    File systemDir = new File(systemPath);
    if (source.getAbsolutePath().contains(systemDir.getAbsolutePath())) {
      String relativeSourcePath = source.getAbsolutePath().replace(systemDir.getAbsolutePath(), "");
      if (!relativeSourcePath.startsWith(File.separator)) {
        relativeSourcePath = File.separator + relativeSourcePath;
      }
      return FilePathUtils.regularizePath(outputBaseDir)
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + relativeSourcePath;
    } else {
      return "";
    }
  }

  public static String getTsFileTmpLinkPath(TsFileResource resource) {
    String absolutePath = resource.getTsFile().getAbsolutePath();
    String dataDir =
        resource
            .getTsFile()
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getAbsolutePath();
    dataDir = FilePathUtils.regularizePath(dataDir);
    return dataDir
        + IoTDBConstant.BACKUP_DATA_TMP_FOLDER_NAME
        + File.separator
        + absolutePath.replace(dataDir, "");
  }

  public static String getSystemFileTmpLinkPath(File source) {
    String absolutePath = source.getAbsolutePath();
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    systemDir = FilePathUtils.regularizePath(systemDir);
    return systemDir
        + IoTDBConstant.BACKUP_SYSTEM_TMP_FOLDER_NAME
        + File.separator
        + absolutePath.replace(systemDir, "");
  }

  public static void moveFile(Path source, Path target) {}

  public static List<File> getAllFilesInOneDir(String path) {
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
}
