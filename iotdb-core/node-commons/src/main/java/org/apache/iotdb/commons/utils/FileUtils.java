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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Arrays;

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

  public static void deleteFileOrDirectory(File file) {
    deleteFileOrDirectory(file, false);
  }

  public static void deleteFileOrDirectory(File file, boolean quietForNoSuchFile) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File subfile : files) {
          deleteFileOrDirectory(subfile, quietForNoSuchFile);
        }
      }
    }
    try {
      Files.delete(file.toPath());
    } catch (NoSuchFileException e) {
      if (!quietForNoSuchFile) {
        LOGGER.warn("{}: {}", e.getMessage(), Arrays.toString(file.list()), e);
      }
    } catch (DirectoryNotEmptyException e) {
      LOGGER.warn("{}: {}", e.getMessage(), Arrays.toString(file.list()), e);
    } catch (Exception e) {
      LOGGER.warn("{}: {}", e.getMessage(), file.getName(), e);
    }
  }

  public static void deleteDirectoryAndEmptyParent(File folder) {
    deleteFileOrDirectory(folder);
    final File parentFolder = folder.getParentFile();
    File[] files = parentFolder.listFiles();
    if (parentFolder.isDirectory() && (files == null || files.length == 0)) {
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
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      synchronized (FileUtils.class) {
        if (!targetDir.exists() && !targetDir.mkdirs()) {
          LOGGER.error(
              "Failed to copy folder, because failed to create target folder[{}].",
              targetDir.getAbsolutePath());
          return false;
        }
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
          int size;
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

  public static void recursivelyDeleteFolder(String path) throws IOException {
    File file = new File(path);
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files == null || files.length == 0) {
        org.apache.commons.io.FileUtils.deleteDirectory(file);
      } else {
        for (File f : files) {
          recursivelyDeleteFolder(f.getAbsolutePath());
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
          recursivelyDeleteFolder(unfinishedTarget.getAbsolutePath());
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

    // Copy
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

    // Rename
    if (!unfinishedTarget.renameTo(target)) {
      LOGGER.error("file rename fail");
      return false;
    }

    // Delete old file
    try {
      if (source.isDirectory()) {
        recursivelyDeleteFolder(source.getAbsolutePath());
      } else {
        org.apache.commons.io.FileUtils.delete(source);
      }
    } catch (IOException e) {
      LOGGER.error("delete source file fail: {}", source.getAbsolutePath(), e);
    }

    LOGGER.info("move file success, {}", fromTo);
    return true;
  }

  public static File createHardLink(File sourceFile, File hardlink) throws IOException {
    if (!hardlink.getParentFile().exists() && !hardlink.getParentFile().mkdirs()) {
      synchronized (FileUtils.class) {
        if (!hardlink.getParentFile().exists() && !hardlink.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  "failed to create hardlink %s for file %s: failed to create parent dir %s",
                  hardlink.getPath(), sourceFile.getPath(), hardlink.getParentFile().getPath()));
        }
      }
    }

    final Path sourcePath = FileSystems.getDefault().getPath(sourceFile.getAbsolutePath());
    final Path linkPath = FileSystems.getDefault().getPath(hardlink.getAbsolutePath());
    Files.createLink(linkPath, sourcePath);
    return hardlink;
  }

  public static File copyFile(File sourceFile, File targetFile) throws IOException {
    if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
      synchronized (FileUtils.class) {
        if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  "failed to copy file %s to %s: failed to create parent dir %s",
                  sourceFile.getPath(),
                  targetFile.getPath(),
                  targetFile.getParentFile().getPath()));
        }
      }
    }

    Files.copy(sourceFile.toPath(), targetFile.toPath());
    return targetFile;
  }

  /**
   * Transfer bytes to human-readable string. Copy from <a
   * href="https://stackoverflow.com/a/3758880">stackoverflow</a>.
   */
  public static String humanReadableByteCountSI(long bytes) {
    if (-1000 < bytes && bytes < 1000) {
      return bytes + " B";
    }
    CharacterIterator ci = new StringCharacterIterator("KMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%.2f %cB", bytes / 1000.0, ci.current());
  }

  public static void moveFileWithMD5Check(final File sourceFile, final File targetDir)
      throws IOException {
    final String sourceFileName = sourceFile.getName();
    final File targetFile = new File(targetDir, sourceFileName);

    if (targetFile.exists()) {
      if (haveSameMD5(sourceFile, targetFile)) {
        org.apache.commons.io.FileUtils.forceDelete(sourceFile);
        LOGGER.info(
            "Deleted the file {} because it already exists in the target directory: {}",
            sourceFile.getName(),
            targetDir.getAbsolutePath());
      } else {
        renameWithMD5(sourceFile, targetDir);
        LOGGER.info(
            "Renamed file {} to {} because it already exists in the target directory: {}",
            sourceFile.getName(),
            targetFile.getName(),
            targetDir.getAbsolutePath());
      }
    } else {
      org.apache.commons.io.FileUtils.moveFileToDirectory(sourceFile, targetDir, true);
    }
  }

  private static boolean haveSameMD5(final File file1, final File file2) {
    try (final InputStream is1 = Files.newInputStream(file1.toPath());
        final InputStream is2 = Files.newInputStream(file2.toPath())) {
      return DigestUtils.md5Hex(is1).equals(DigestUtils.md5Hex(is2));
    } catch (final Exception e) {
      return false;
    }
  }

  private static void renameWithMD5(File sourceFile, File targetDir) throws IOException {
    try (final InputStream is = Files.newInputStream(sourceFile.toPath())) {
      final String sourceFileBaseName = FilenameUtils.getBaseName(sourceFile.getName());
      final String sourceFileExtension = FilenameUtils.getExtension(sourceFile.getName());
      final String sourceFileMD5 = DigestUtils.md5Hex(is);

      final String targetFileName =
          sourceFileBaseName + "-" + sourceFileMD5.substring(0, 16) + "." + sourceFileExtension;
      final File targetFile = new File(targetDir, targetFileName);

      org.apache.commons.io.FileUtils.moveFile(
          sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
