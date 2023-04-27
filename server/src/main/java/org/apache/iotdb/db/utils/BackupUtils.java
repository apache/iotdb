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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class BackupUtils {
  private static String confDir;

  public static boolean checkConfDir() {
    String iotdbConf = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (iotdbConf != null) {
      confDir = iotdbConf;
      return true;
    }
    String iotdbHome = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (iotdbHome != null) {
      confDir = iotdbHome + File.separator + "conf";
      return true;
    }
    return false;
  }

  public static String getConfDir() {
    return confDir;
  }

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

  public static String getTsFileTargetPath(File tsFile, String outputBaseDir)
      throws IOException {
    String tsFileDataDir =
            tsFile
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getParentFile()
            .getAbsolutePath();
    Path tsFileDataDirPath = Paths.get(FilePathUtils.regularizePath(tsFileDataDir));
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    if (dataDirs.length == 1) {
      return FilePathUtils.regularizePath(outputBaseDir)
          + "data"
          + File.separator
          + "data"
          + tsFile.getAbsolutePath().replace(tsFileDataDir, "");
    } else {
      for (int i = 0; i < dataDirs.length; ++i) {
        Path dataDirPath = Paths.get(dataDirs[i]);
        if (Files.isSameFile(dataDirPath, tsFileDataDirPath)) {
          return FilePathUtils.regularizePath(outputBaseDir)
              + "data"
              + File.separator
              + "data"
              + i
              + tsFile.getAbsolutePath().replace(tsFileDataDir, "");
        }
      }
    }
    throw new IOException(
        "TsFile " + tsFile.getAbsolutePath() + " does not match any data directory.");
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
          + "data"
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + relativeSourcePath;
    } else {
      return "";
    }
  }

  public static String getConfigFileTargetPath(File source, String outputBaseDir) {
    return FilePathUtils.regularizePath(outputBaseDir) + "conf" + File.separator + source.getName();
  }

  public static String getTsFileTmpLinkPath(File tsFile) {
    String absolutePath = tsFile.getAbsolutePath();
    String dataDir =
            tsFile
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
    systemDir = new File(systemDir).getAbsolutePath();
    systemDir = FilePathUtils.regularizePath(systemDir);
    return systemDir
        + IoTDBConstant.BACKUP_SYSTEM_TMP_FOLDER_NAME
        + File.separator
        + absolutePath.replace(systemDir, "");
  }

  public static void copyFile(Path source, Path target) throws IOException {
    Files.copy(source, target);
  }

  public static List<File> getAllFilesInOneDir(String path) {
    List<File> sonFiles = new ArrayList<>();
    File[] sonFileAndDirs = new File(path).listFiles();
    if (sonFileAndDirs == null) {
      return sonFiles;
    }
    for (File sonFile : sonFileAndDirs) {
      if (sonFile.isFile()) {
        sonFiles.add(sonFile);
      } else {
        sonFiles.addAll(getAllFilesInOneDir(sonFile.getAbsolutePath()));
      }
    }
    return sonFiles;
  }

  public static List<File> getAllFilesWithSuffixInOneDir(String path, String suffix) {
    List<File> sonFiles = new ArrayList<>();
    File[] sonFileAndDirs = new File(path).listFiles();
    if (sonFileAndDirs == null) {
      return sonFiles;
    }
    for (File sonFile : sonFileAndDirs) {
      if (sonFile.isFile() && sonFile.getName().endsWith(suffix)) {
        sonFiles.add(sonFile);
      } else {
        sonFiles.addAll(getAllFilesWithSuffixInOneDir(sonFile.getAbsolutePath(), suffix));
      }
    }
    return sonFiles;
  }

  public static boolean deleteBackupTmpDir() {
    boolean success = true;
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      File dataTmpDir =
          new File(
              FilePathUtils.regularizePath(dataDir) + IoTDBConstant.BACKUP_DATA_TMP_FOLDER_NAME);
      success = success && deleteFileOrDirRecursively(dataTmpDir);
    }
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    File systemTmpDir =
        new File(
            FilePathUtils.regularizePath(systemDir) + IoTDBConstant.BACKUP_SYSTEM_TMP_FOLDER_NAME);
    return success && deleteFileOrDirRecursively(systemTmpDir);
  }

  /** Will return true when the file is deleted successfully or when it does not exist. */
  public static boolean deleteFileOrDirRecursively(File file) {
    if (file == null || !file.exists()) return true;
    if (!file.isFile()) {
      File[] sonFileAndDirs = file.listFiles();
      if (sonFileAndDirs != null) {
        for (File sonFile : sonFileAndDirs) {
          deleteFileOrDirRecursively(sonFile);
        }
      }
    }
    return file.delete();
  }
}
