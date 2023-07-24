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
import java.util.Objects;

public class FileUtils {
  private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

  private static final int BUFFER_SIZE = 1024;

  private FileUtils() {}

  public static void deleteDirectory(File folder) {
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        deleteDirectory(file);
      }
    }
    try {
      Files.delete(folder.toPath());
    } catch (NoSuchFileException | DirectoryNotEmptyException e) {
      logger.warn("{}: {}", e.getMessage(), Arrays.toString(folder.list()), e);
    } catch (Exception e) {
      logger.warn("{}: {}", e.getMessage(), folder.getName(), e);
    }
  }

  public static void deleteDirectoryAndEmptyParent(File folder) {
    deleteDirectory(folder);
    final File parentFolder = folder.getParentFile();
    if (parentFolder.isDirectory()
        && Objects.requireNonNull(parentFolder.listFiles()).length == 0) {
      if (!parentFolder.delete()) {
        logger.warn("Delete folder failed: {}", parentFolder.getAbsolutePath());
      }
    }
  }

  public static boolean copyDir(File sourceDir, File targetDir) throws IOException {
    if (!sourceDir.exists() || !sourceDir.isDirectory()) {
      logger.error(
          "Failed to copy folder, because source folder [{}] doesn't exist.",
          sourceDir.getAbsolutePath());
      return false;
    }
    if (!targetDir.exists()) {
      if (!targetDir.mkdirs()) {
        logger.error(
            "Failed to copy folder, because failed to create target folder[{}].",
            targetDir.getAbsolutePath());
        return false;
      }
    } else if (!targetDir.isDirectory()) {
      logger.error(
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
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(targetFile))) {
          byte[] bytes = new byte[BUFFER_SIZE];
          int size = 0;
          while ((size = in.read(bytes)) > 0) {
            out.write(bytes, 0, size);
          }
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
}
