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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Maps a TsFile, a .resource file or a .mod file to the pipe directory that holds its hardlink or
 * copy, i.e. {@code <dataDir>/<pipe hardlink base dir>/<pipe hardlink tsfile dir>[/<pipeName>]} of
 * the data directory that contains the file.
 *
 * <p>A hardlink can only be created on the same file system, and the TsFiles of one DataRegion may
 * be spread over multiple disks, so the directory is resolved per file.
 */
class PipeTsFileDirPathMapping {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileDirPathMapping.class);

  /** Canonical path of a local data directory -> canonical path of its pipe base directory. */
  private final Map<String, String> dataDirPathToPipeBaseDirPath = new LinkedHashMap<>();

  private final String pipeHardlinkBaseDirName;
  private final String pipeHardlinkTsFileDirName;

  PipeTsFileDirPathMapping(
      final @Nonnull String[] localDataDirs,
      final @Nonnull String pipeHardlinkBaseDirName,
      final @Nonnull String pipeHardlinkTsFileDirName) {
    this.pipeHardlinkBaseDirName = pipeHardlinkBaseDirName;
    this.pipeHardlinkTsFileDirName = pipeHardlinkTsFileDirName;

    for (final String dataDir : localDataDirs) {
      final String canonicalDataDir = canonicalDataDirPath(dataDir);
      dataDirPathToPipeBaseDirPath.put(
          canonicalDataDir, canonicalDataDir + File.separator + pipeHardlinkBaseDirName);
    }
    LOGGER.info(
        DataNodePipeMessages.LOG_PIPE_HARDLINK_DIRECTORIES_ARE_RESOLVED_AS_ARG_1DAB3D0F,
        dataDirPathToPipeBaseDirPath);
  }

  /**
   * Returns the hardlink or the copied file in the pipe directory of {@code pipeName}, or in the
   * public pipe directory if {@code pipeName} is {@code null}.
   */
  File getHardlinkOrCopiedFile(final File file, final @Nullable String pipeName)
      throws IOException {
    final File pipeTsFileDir = getPipeTsFileDir(file, pipeName);
    final File parent = file.getAbsoluteFile().getParentFile();
    if (Objects.nonNull(parent) && canonicalPath(parent).equals(pipeTsFileDir.getPath())) {
      // The file is already the pipe file of this scope, e.g. one left by a previous process. It is
      // reused, and must not be linked or copied onto itself.
      return file;
    }
    return new File(pipeTsFileDir, getRelativeFilePath(file));
  }

  private File getPipeTsFileDir(final File file, final @Nullable String pipeName)
      throws IOException {
    // Canonical, so that a symlinked data dir yields one path per physical file.
    final File tsFileDir =
        new File(getPipeBaseDir(file).getCanonicalFile(), pipeHardlinkTsFileDirName);
    return Objects.isNull(pipeName) ? tsFileDir : new File(tsFileDir, pipeName);
  }

  /** Returns the pipe base directory of the file, which is always on the disk of the file. */
  private File getPipeBaseDir(final File file) throws IOException {
    // Canonical, so that a symlinked data dir yields one path per physical file. A path which can
    // not be resolved must fail the resolution: a pin on a non-canonical path would break the "one
    // path per physical file" invariant of the reference count map.
    final String matchedDataDir = findLongestDataDir(canonicalPath(file));
    if (Objects.nonNull(matchedDataDir)) {
      return new File(matchedDataDir, pipeHardlinkBaseDirName);
    }

    final File existingPipeBaseDir = getExistingPipeBaseDir(file);
    if (Objects.nonNull(existingPipeBaseDir)) {
      return existingPipeBaseDir;
    }

    // The file is not below a local data directory, e.g. it has been unloaded, so the pipe
    // directory goes next to the data directory layout the file still belongs to.
    for (File dir = file.getAbsoluteFile().getParentFile();
        Objects.nonNull(dir);
        dir = dir.getParentFile()) {
      if (isDataDirAnchor(dir) && Objects.nonNull(dir.getParentFile())) {
        return new File(dir.getParentFile(), pipeHardlinkBaseDirName);
      }
    }
    throw failedToGetPipeDir(file, null);
  }

  /** Returns the pipe base directory the file is already in, or {@code null}. */
  private @Nullable File getExistingPipeBaseDir(final @Nonnull File file) {
    File dir = file.getAbsoluteFile().getParentFile();
    // <pipe base dir>/<pipe hardlink tsfile dir>[/<pipeName>]/<file>
    for (int depth = 0; depth < 2 && Objects.nonNull(dir); ++depth, dir = dir.getParentFile()) {
      final File baseDir = dir.getParentFile();
      if (dir.getName().equals(pipeHardlinkTsFileDirName)
          && Objects.nonNull(baseDir)
          && baseDir.getName().equals(pipeHardlinkBaseDirName)) {
        return baseDir;
      }
    }
    return null;
  }

  /** Returns the name of the pipe file, which is unique among the files of one pipe. */
  private String getRelativeFilePath(final @Nonnull File file) {
    if (Objects.nonNull(getExistingPipeBaseDir(file))) {
      // The pipe file directories are flat.
      return file.getName();
    }

    final StringBuilder builder = new StringBuilder(file.getName());
    for (File currentFile = file;
        !isDataDirAnchor(currentFile) && Objects.nonNull(currentFile.getParentFile());
        currentFile = currentFile.getParentFile()) {
      builder.insert(0, currentFile.getParentFile().getName() + IoTDBConstant.FILE_NAME_SEPARATOR);
    }
    return builder.toString();
  }

  private boolean isDataDirAnchor(final @Nonnull File file) {
    final String name = file.getName();
    return name.equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        || name.equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
        || name.equals(pipeHardlinkBaseDirName);
  }

  /////////////////////////// Utils ///////////////////////////

  private String findLongestDataDir(final @Nonnull String path) {
    String matched = null;
    for (final String dataDirPath : dataDirPathToPipeBaseDirPath.keySet()) {
      if (path.startsWith(dataDirPath + File.separator)
          && (Objects.isNull(matched) || dataDirPath.length() > matched.length())) {
        matched = dataDirPath;
      }
    }
    return matched;
  }

  private static String canonicalPath(final @Nonnull File file) throws IOException {
    // getCanonicalPath is called on the File itself, so that an overridden implementation (and its
    // failure) is neither lost nor turned into a non-canonical path.
    try {
      return file.getCanonicalPath();
    } catch (final IOException e) {
      throw failedToGetPipeDir(file, e);
    }
  }

  /**
   * Returns the canonical path of a configured data directory, falling back to its absolute path if
   * it can not be resolved. The configuration of a directory must not fail the pin of a file.
   */
  private static String canonicalDataDirPath(final @Nonnull String dataDir) {
    final File dataDirFile = new File(dataDir);
    try {
      return dataDirFile.getCanonicalPath();
    } catch (final IOException e) {
      return dataDirFile.getAbsolutePath();
    }
  }

  private static IOException failedToGetPipeDir(
      final @Nonnull File file, final @Nullable IOException cause) {
    return new IOException(
        String.format(
            DataNodePipeMessages
                .PIPE_EXCEPTION_FAILED_TO_GET_HARDLINK_OR_COPIED_FILE_IN_PIPE_DIR_FOR_FILE_F009D86E,
            file.getPath()),
        cause);
  }
}
