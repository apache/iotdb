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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.file.SystemFileFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private static final int BUFFER_SIZE = 1024;

  private FileUtils() {}

  public static boolean deleteFileIfExist(File file) {
    try {
      Files.deleteIfExists(file.toPath());
      return true;
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
  }

  public static void deleteDirectory(File folder) {
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        deleteDirectory(file);
      }
    }
    try {
      Files.delete(folder.toPath());
    } catch (NoSuchFileException | DirectoryNotEmptyException e) {
      LOGGER.warn("{}: {}", e.getMessage(), Arrays.toString(folder.list()), e);
    } catch (Exception e) {
      LOGGER.warn("{}: {}", e.getMessage(), folder.getName(), e);
    }
  }

  public static void deleteDirectoryAndEmptyParent(File folder) {
    deleteDirectory(folder);
    final File parentFolder = folder.getParentFile();
    if (parentFolder.isDirectory()
        && Objects.requireNonNull(parentFolder.listFiles()).length == 0) {
      if (!parentFolder.delete()) {
        LOGGER.warn("Delete folder failed: {}", parentFolder.getAbsolutePath());
      }
    }
  }

  public static boolean copyDir(File sourceDir, File targetDir) throws IOException {
    if (!sourceDir.exists() || !sourceDir.isDirectory()) {
      LOGGER.error(
          "Failed to copy folder, because source folder [{}] doesn't exist.",
          sourceDir.getAbsolutePath());
      return false;
    }
    if (!targetDir.exists()) {
      if (!targetDir.mkdirs()) {
        LOGGER.error(
            "Failed to copy folder, because failed to create target folder[{}].",
            targetDir.getAbsolutePath());
        return false;
      }
    } else if (!targetDir.isDirectory()) {
      LOGGER.error(
          "Failed to copy folder, because target folder [{}] already exist.",
          targetDir.getAbsolutePath());
      return false;
    }
    File[] files = sourceDir.listFiles();
    if (files == null || files.length == 0) {
      return true;
    }
    boolean result = true;
    for (File file : files) {
      if (!file.exists()) {
        continue;
      }
      File targetFile = new File(targetDir, file.getName());
      if (file.isDirectory()) {
        result &= copyDir(file.getAbsoluteFile(), targetFile);
      } else {
        // copy file
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
            FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
            BufferedOutputStream out = new BufferedOutputStream(fileOutputStream)) {
          byte[] bytes = new byte[BUFFER_SIZE];
          int size = 0;
          while ((size = in.read(bytes)) > 0) {
            out.write(bytes, 0, size);
          }
          out.flush();
          fileOutputStream.getFD().sync(); // after try block, stream will call close automatically
        } catch (IOException e) {
          LOGGER.warn("get ioexception on file {}", file.getAbsolutePath(), e);
          throw e;
        }
      }
    }
    return result;
  }

  /**
   * Calculate the directory size including sub dir.
   *
   * @param path
   * @return
   */
  public static long getDirSize(String path) {
    long sum = 0;
    File file = SystemFileFactory.INSTANCE.getFile(path);
    if (file.isDirectory()) {
      String[] list = file.list();
      for (String item : list) {
        String subPath = path + File.separator + item;
        sum += getDirSize(subPath);
      }
    } else {
      // this is a file.
      sum += file.length();
    }
    return sum;
  }

  public static void recursiveDeleteFolder(String path) throws IOException {
    File file = new File(path);
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files == null || files.length == 0) {
        org.apache.commons.io.FileUtils.deleteDirectory(file);
      } else {
        for (File f : files) {
          recursiveDeleteFolder(f.getAbsolutePath());
        }
        org.apache.commons.io.FileUtils.deleteDirectory(file);
      }
    } else {
      org.apache.commons.io.FileUtils.delete(file);
    }
  }

  /** Add a prefix to a relative path file */
  public static String addPrefix2FilePath(String prefix, String file) {
    if (!new File(file).isAbsolute() && prefix != null && prefix.length() > 0) {
      if (!prefix.endsWith(File.separator)) {
        file = prefix + File.separatorChar + file;
      } else {
        file = prefix + file;
      }
    }
    return file;
  }

  /**
   * Move source file to target file. The move will be divided into three steps: 1. Copy the source
   * file to the "target.unfinished" location 2. Rename the "target.unfinished" to "target" 3.
   * Delete the source file
   *
   * @param source source file, which can be a directory.
   * @param target target file, which can be a directory.
   * @return success or not
   */
  public static boolean moveFileSafe(File source, File target) {
    if (target.exists()) {
      LOGGER.info(
          "won't move file again because target file already exists: {}", target.getAbsolutePath());
      LOGGER.info("you may manually delete source file if necessary: {}", source.getAbsolutePath());
      return true;
    }

    final String fromTo =
        String.format("from %s to %s", source.getAbsolutePath(), target.getAbsolutePath());
    LOGGER.info("start to move file, {}", fromTo);

    // Prepare the xxx.unfinished File, delete it if it's already exist
    File unfinishedTarget = new File(target.getAbsolutePath() + ".unfinished");
    try {
      if (unfinishedTarget.exists()) {
        if (unfinishedTarget.isFile()) {
          org.apache.commons.io.FileUtils.delete(unfinishedTarget);
        } else {
          recursiveDeleteFolder(unfinishedTarget.getAbsolutePath());
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          "delete unfinished target file failed: {}", unfinishedTarget.getAbsolutePath(), e);
      return false;
    }
    LOGGER.info(
        "unfinished target file which was created last time has been deleted: {}",
        unfinishedTarget.getAbsolutePath());

    // copy
    try {
      if (source.isDirectory()) {
        if (!copyDir(source, unfinishedTarget)) {
          LOGGER.error("file copy fail");
          return false;
        }
      } else {
        org.apache.commons.io.FileUtils.copyFile(source, unfinishedTarget);
      }
    } catch (IOException e) {
      LOGGER.error("file copy fail", e);
      return false;
    }

    // rename
    if (!unfinishedTarget.renameTo(target)) {
      LOGGER.error("file rename fail");
      return false;
    }

    // delete old file
    try {
      if (source.isDirectory()) {
        recursiveDeleteFolder(source.getAbsolutePath());
      } else {
        org.apache.commons.io.FileUtils.delete(source);
      }
    } catch (IOException e) {
      LOGGER.error("delete source file fail: {}", source.getAbsolutePath(), e);
    }

    LOGGER.info("move file success, {}", fromTo);
    return true;
  }

  public static void logBreakpoint(String logContent) {
    if (CommonDescriptor.getInstance().getConfig().isIntegrationTest()) {
      logBreakpointImpl(logContent);
    }
  }

  public static <T extends Enum<T>> void logBreakpoint(T x) {
    if (CommonDescriptor.getInstance().getConfig().isIntegrationTest()) {
      logBreakpointImpl(enumToString(x));
    }
  }

  public static <T extends Enum<T>> String enumToString(T x) {
    return x.getClass().getSimpleName() + "." + x.name();
  }

  /**
   * @param s something like "[a, b, c]"
   * @return List[a,b,c]
   */
  public static Set<String> parseKillPoints(String s) {
    LOGGER.info("raw kill point:{}", s);
    if (s == null) {
      LOGGER.info("No kill point");
      return new HashSet<>();
    }
    Set<String> result =
        Arrays.stream(s.replace("[", "").replace("]", "").replace(" ", "").split(","))
            .collect(Collectors.toSet());
    LOGGER.info("Kill point set: {}", result);
    return result;
  }

  private static void logBreakpointImpl(String breakPointName) {
    if (CommonDescriptor.getInstance()
        .getConfig()
        .getEnabledKillPoints()
        .contains(breakPointName)) {
      LOGGER.info("Kill point: {}", breakPointName);
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
