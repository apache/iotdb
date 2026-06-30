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
import org.apache.iotdb.commons.i18n.UtilMessages;

import org.apache.tsfile.external.commons.codec.digest.DigestUtils;
import org.apache.tsfile.external.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private static final int BUFFER_SIZE = 1024;

  private FileUtils() {}

  public static List<File> listFilesRecursively(File dir, FileFilter fileFilter) {
    List<File> result = new ArrayList<>();
    Stack<File> stack = new Stack<>();
    if (dir.exists()) {
      stack.push(dir);
    }
    while (!stack.isEmpty()) {
      File file = stack.pop();
      if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files != null) {
          for (File f : files) {
            stack.push(f);
          }
        }
      }
      if (fileFilter.accept(file)) {
        result.add(file);
      }
    }
    return result;
  }

  public static boolean deleteFileIfExist(File file) {
    try {
      Files.deleteIfExists(file.toPath());
      return true;
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
  }

  public static void createLink(Path link, Path existing, boolean fallBackToCopy)
      throws IOException {
    try {
      Files.createLink(link, existing);
    } catch (IOException | UnsupportedOperationException e) {
      if (!fallBackToCopy) {
        throw e;
      }
      try {
        Files.copy(existing, link);
      } catch (IOException copyException) {
        copyException.addSuppressed(e);
        throw copyException;
      }
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

  public static void deleteFileOrDirectoryWithRetry(File file) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File subfile : files) {
          deleteFileOrDirectoryWithRetry(subfile);
        }
      }
    }
    try {
      RetryUtils.retryOnException(
          () -> {
            Files.delete(file.toPath());
            return null;
          });
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
        LOGGER.warn(UtilMessages.DELETE_FOLDER_FAILED, parentFolder.getAbsolutePath());
      }
    }
  }

  public static boolean copyDir(File sourceDir, File targetDir) throws IOException {
    if (!sourceDir.exists() || !sourceDir.isDirectory()) {
      LOGGER.error(UtilMessages.COPY_FOLDER_SOURCE_NOT_EXIST, sourceDir.getAbsolutePath());
      return false;
    }
    if (!targetDir.exists() && !targetDir.mkdirs()) {
      synchronized (FileUtils.class) {
        if (!targetDir.exists() && !targetDir.mkdirs()) {
          LOGGER.error(UtilMessages.COPY_FOLDER_CREATE_TARGET_FAILED, targetDir.getAbsolutePath());
          return false;
        }
      }
    } else if (!targetDir.isDirectory()) {
      LOGGER.error(UtilMessages.COPY_FOLDER_TARGET_ALREADY_EXISTS, targetDir.getAbsolutePath());
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
          LOGGER.warn(UtilMessages.IO_EXCEPTION_ON_FILE, file.getAbsolutePath(), e);
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
        org.apache.tsfile.external.commons.io.FileUtils.deleteDirectory(file);
      } else {
        for (File f : files) {
          recursivelyDeleteFolder(f.getAbsolutePath());
        }
        org.apache.tsfile.external.commons.io.FileUtils.deleteDirectory(file);
      }
    } else {
      org.apache.tsfile.external.commons.io.FileUtils.delete(file);
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
      LOGGER.info(UtilMessages.MOVE_FILE_TARGET_ALREADY_EXISTS, target.getAbsolutePath());
      LOGGER.info(UtilMessages.MOVE_FILE_DELETE_SOURCE_HINT, source.getAbsolutePath());
      return true;
    }

    final String fromTo =
        String.format("from %s to %s", source.getAbsolutePath(), target.getAbsolutePath());
    LOGGER.info(UtilMessages.MOVE_FILE_START, fromTo);

    // Prepare the xxx.unfinished File, delete it if it's already exist
    File unfinishedTarget = new File(target.getAbsolutePath() + ".unfinished");
    try {
      if (unfinishedTarget.exists()) {
        if (unfinishedTarget.isFile()) {
          org.apache.tsfile.external.commons.io.FileUtils.delete(unfinishedTarget);
        } else {
          recursivelyDeleteFolder(unfinishedTarget.getAbsolutePath());
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          UtilMessages.DELETE_UNFINISHED_TARGET_FAILED, unfinishedTarget.getAbsolutePath(), e);
      return false;
    }
    LOGGER.info(UtilMessages.UNFINISHED_TARGET_DELETED, unfinishedTarget.getAbsolutePath());

    // Copy
    try {
      if (source.isDirectory()) {
        if (!copyDir(source, unfinishedTarget)) {
          LOGGER.error(UtilMessages.FILE_COPY_FAIL);
          return false;
        }
      } else {
        org.apache.tsfile.external.commons.io.FileUtils.copyFile(source, unfinishedTarget);
      }
    } catch (IOException e) {
      LOGGER.error(UtilMessages.FILE_COPY_FAIL, e);
      return false;
    }

    // Rename
    if (!unfinishedTarget.renameTo(target)) {
      LOGGER.error(UtilMessages.FILE_RENAME_FAIL);
      return false;
    }

    // Delete old file
    try {
      if (source.isDirectory()) {
        recursivelyDeleteFolder(source.getAbsolutePath());
      } else {
        org.apache.tsfile.external.commons.io.FileUtils.delete(source);
      }
    } catch (IOException e) {
      LOGGER.error(UtilMessages.DELETE_SOURCE_FILE_FAIL, source.getAbsolutePath(), e);
    }

    LOGGER.info(UtilMessages.MOVE_FILE_SUCCESS, fromTo);
    return true;
  }

  public static File createHardLink(File sourceFile, File hardlink) throws IOException {
    if (!hardlink.getParentFile().exists() && !hardlink.getParentFile().mkdirs()) {
      synchronized (FileUtils.class) {
        if (!hardlink.getParentFile().exists() && !hardlink.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  UtilMessages.FAILED_TO_CREATE_HARDLINK_PARENT_DIR,
                  hardlink.getPath(),
                  sourceFile.getPath(),
                  hardlink.getParentFile().getPath()));
        }
      }
    }

    final Path sourcePath = FileSystems.getDefault().getPath(sourceFile.getAbsolutePath());
    final Path linkPath = FileSystems.getDefault().getPath(hardlink.getAbsolutePath());
    try {
      Files.createLink(linkPath, sourcePath);
    } catch (final FileAlreadyExistsException fileAlreadyExistsException) {
      if (haveSameMD5(sourceFile, hardlink)) {
        LOGGER.warn(
            UtilMessages.HARDLINK_ALREADY_EXISTS,
            hardlink.getAbsolutePath(),
            sourceFile.getAbsolutePath());
      } else {
        LOGGER.warn(
            UtilMessages.HARDLINK_MISMATCH_RETRY,
            hardlink.getAbsolutePath(),
            sourceFile.getAbsolutePath());
        deleteFileIfExist(hardlink);
        try {
          Files.createLink(linkPath, sourcePath);
        } catch (final Exception e) {
          deleteFileIfExist(linkPath.toFile());
          LOGGER.error(
              UtilMessages.FAILED_TO_CREATE_HARDLINK,
              hardlink.getAbsolutePath(),
              sourceFile.getAbsolutePath(),
              e.getMessage(),
              e);
          throw e;
        }
      }
    }
    return hardlink;
  }

  public static File copyFile(File sourceFile, File targetFile) throws IOException {
    if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
      synchronized (FileUtils.class) {
        if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  UtilMessages.FAILED_TO_COPY_FILE_PARENT_DIR,
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
      moveFile(sourceFile, targetDir);
    } else {
      org.apache.tsfile.external.commons.io.FileUtils.moveFileToDirectory(
          sourceFile, targetDir, true);
    }
  }

  private static void moveFile(File sourceFile, File targetDir) throws IOException {
    String sourceFileName = sourceFile.getName();
    final File exitsFile = new File(targetDir, sourceFileName);

    // First check file sizes
    long sourceFileSize = sourceFile.length();
    long existsFileSize = exitsFile.length();

    if (sourceFileSize != existsFileSize) {
      File file = renameWithSize(sourceFile, sourceFileSize, targetDir);
      if (!file.exists()) {
        moveFileRename(sourceFile, file);
      }
      return;
    }

    // If sizes are equal, check MD5
    String sourceFileMD5;
    String existsFileMD5;
    try (final FileInputStream is1 = new FileInputStream(sourceFile);
        final FileInputStream is2 = new FileInputStream(exitsFile); ) {
      sourceFileMD5 = DigestUtils.md5Hex(is1);
      existsFileMD5 = DigestUtils.md5Hex(is2);
    }

    if (sourceFileMD5.equals(existsFileMD5)) {
      org.apache.tsfile.external.commons.io.FileUtils.forceDelete(sourceFile);
      LOGGER.info(
          UtilMessages.DELETED_DUPLICATE_FILE, sourceFile.getName(), targetDir.getAbsolutePath());
    } else {
      File file = renameWithMD5(sourceFile, sourceFileMD5, targetDir);
      moveFileRename(sourceFile, file);
    }
  }

  public static void copyFileWithMD5Check(final File sourceFile, final File targetDir)
      throws IOException {
    final String sourceFileName = sourceFile.getName();
    final File targetFile = new File(targetDir, sourceFileName);
    if (targetFile.exists()) {
      copyFileWithMD5(sourceFile, targetDir);
    } else {
      try {
        Files.createDirectories(targetDir.toPath());
      } catch (IOException e) {
        LOGGER.warn(UtilMessages.FAILED_TO_CREATE_TARGET_DIRECTORY, targetDir.getAbsolutePath());
        throw e;
      }

      Files.copy(
          sourceFile.toPath(),
          targetFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  private static File renameWithMD5(
      final File sourceFile, final String sourceFileMD5, final File targetDir) throws IOException {
    final String sourceFileBaseName = FilenameUtils.getBaseName(sourceFile.getName());
    final String sourceFileExtension = FilenameUtils.getExtension(sourceFile.getName());

    final String targetFileName =
        sourceFileBaseName + "-" + sourceFileMD5.substring(0, 16) + "." + sourceFileExtension;
    return new File(targetDir, targetFileName);
  }

  private static void copyFileWithMD5(final File sourceFile, final File targetDir)
      throws IOException {
    String sourceFileName = sourceFile.getName();
    final File exitsFile = new File(targetDir, sourceFileName);

    // First check file sizes
    long sourceFileSize = sourceFile.length();
    long exitsFileSize = exitsFile.length();

    if (sourceFileSize != exitsFileSize) {
      File file = renameWithSize(sourceFile, sourceFileSize, targetDir);
      if (!file.exists()) {
        copyFileRename(sourceFile, file);
      }
      return;
    }

    // If sizes are equal, check MD5
    String sourceFileMD5;
    String exitsFileMD5;
    try (final FileInputStream is1 = new FileInputStream(sourceFile);
        final FileInputStream is2 = new FileInputStream(exitsFile); ) {
      sourceFileMD5 = DigestUtils.md5Hex(is1);
      exitsFileMD5 = DigestUtils.md5Hex(is2);
    }

    if (sourceFileMD5.equals(exitsFileMD5)) {
      return;
    }

    File file = renameWithMD5(sourceFile, sourceFileMD5, targetDir);
    if (!file.exists()) {
      copyFileRename(sourceFile, file);
    }
  }

  private static File renameWithSize(
      final File sourceFile, final long sourceFileSize, final File targetDir) {
    final String sourceFileBaseName = FilenameUtils.getBaseName(sourceFile.getName());
    final String sourceFileExtension = FilenameUtils.getExtension(sourceFile.getName());

    // If the file sizes are different, rename the source file by appending its size and the
    // current timestamp
    final String newFileName =
        String.format(
            "%s_%s_%s.%s",
            sourceFileBaseName, sourceFileSize, System.currentTimeMillis(), sourceFileExtension);

    return new File(targetDir, newFileName);
  }

  private static boolean haveSameMD5(final File file1, final File file2) {
    try (final InputStream is1 = Files.newInputStream(file1.toPath());
        final InputStream is2 = Files.newInputStream(file2.toPath())) {
      return DigestUtils.md5Hex(is1).equals(DigestUtils.md5Hex(is2));
    } catch (final Exception e) {
      return false;
    }
  }

  private static void moveFileRename(File sourceFile, File targetFile) throws IOException {
    org.apache.tsfile.external.commons.io.FileUtils.moveFile(
        sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);

    LOGGER.info(
        UtilMessages.RENAMED_FILE_ALREADY_EXISTS,
        sourceFile.getName(),
        targetFile.getName(),
        targetFile.getParentFile().getAbsolutePath());
  }

  private static void copyFileRename(final File sourceFile, final File targetFile)
      throws IOException {
    Files.copy(
        sourceFile.toPath(),
        targetFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.COPY_ATTRIBUTES);

    LOGGER.info(
        UtilMessages.COPIED_FILE_ALREADY_EXISTS,
        sourceFile.getName(),
        targetFile,
        targetFile.getParentFile().getAbsolutePath());
  }

  public static String getIllegalError4Directory(final String path) {
    if (path == null || path.isEmpty()) {
      return UtilMessages.ILLEGAL_EMPTY_PATH;
    }
    if (path.equals(".") || path.equals("..") || path.contains("/") || path.contains("\\")) {
      return UtilMessages.ILLEGAL_PATH_DOTS_OR_SEPARATORS;
    }
    if (!WindowsOSUtils.isLegalPathSegment4Windows(path)) {
      return WindowsOSUtils.OS_SEGMENT_ERROR;
    }
    return null;
  }
}
