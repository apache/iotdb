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
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class PipeTsFileResourceManager {

  private final Map<String, Integer> hardlinkOrCopiedFileToReferenceMap = new HashMap<>();

  /**
   * given a file, create a hardlink or copy it to pipe dir, maintain a reference count for the
   * hardlink or copied file, and return the hardlink or copied file.
   *
   * <p>if the given file is already a hardlink or copied file, increase its reference count and
   * return it.
   *
   * <p>if the given file is a tsfile, create a hardlink in pipe dir, increase the reference count
   * of the hardlink and return it.
   *
   * <p>otherwise, copy the file (.mod or .resource) to pipe dir, increase the reference count of
   * the copied file and return it.
   *
   * @param file tsfile, resource file or mod file. can be original file or hardlink/copy of
   *     original file
   * @param isTsFile true to create hardlink, false to copy file
   * @return the hardlink or copied file
   * @throws IOException when create hardlink or copy file failed
   */
  public synchronized File increaseFileReference(File file, boolean isTsFile) throws IOException {
    // if the file is already a hardlink or copied file, just increase reference count and return it
    if (increaseReferenceIfExists(file.getPath())) {
      return file;
    }

    // if the file is not a hardlink or copied file, check if there is a related hardlink or copied
    // file in pipe dir. if so, increase reference count and return it
    final File hardlinkOrCopiedFile = getHardlinkOrCopiedFileInPipeDir(file);
    if (increaseReferenceIfExists(hardlinkOrCopiedFile.getPath())) {
      return hardlinkOrCopiedFile;
    }

    // if the file is not a hardlink or copied file, and there is no related hardlink or copied
    // file in pipe dir, create a hardlink or copy it to pipe dir, maintain a reference count for
    // the hardlink or copied file, and return the hardlink or copied file.
    hardlinkOrCopiedFileToReferenceMap.put(hardlinkOrCopiedFile.getPath(), 1);
    // if the file is a tsfile, create a hardlink in pipe dir and return it.
    // otherwise, copy the file (.mod or .resource) to pipe dir and return it.
    return isTsFile
        ? createHardLink(file, hardlinkOrCopiedFile)
        : copyFile(file, hardlinkOrCopiedFile);
  }

  private boolean increaseReferenceIfExists(String path) {
    hardlinkOrCopiedFileToReferenceMap.computeIfPresent(path, (key, value) -> value + 1);
    return hardlinkOrCopiedFileToReferenceMap.containsKey(path);
  }

  private static File getHardlinkOrCopiedFileInPipeDir(File file) throws IOException {
    try {
      return new File(getPipeTsFileDirPath(file), getRelativeFilePath(file));
    } catch (Exception e) {
      throw new IOException(
          String.format(
              "failed to get hardlink or copied file in pipe dir "
                  + "for file %s, it is not a tsfile, mod file or resource file",
              file.getPath()),
          e);
    }
  }

  private static String getPipeTsFileDirPath(File file) throws IOException {
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)) {
      file = file.getParentFile();
    }
    return file.getParentFile().getCanonicalPath()
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkTsFileDirName();
  }

  private static String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)) {
      file = file.getParentFile();
      builder =
          new StringBuilder(file.getName())
              .append(IoTDBConstant.FILE_NAME_SEPARATOR)
              .append(builder);
    }
    return builder.toString();
  }

  private static File createHardLink(File sourceFile, File hardlink) throws IOException {
    if (!hardlink.getParentFile().exists() && !hardlink.getParentFile().mkdirs()) {
      throw new IOException(
          String.format(
              "failed to create hardlink %s for file %s: failed to create parent dir %s",
              hardlink.getPath(), sourceFile.getPath(), hardlink.getParentFile().getPath()));
    }

    final Path sourcePath = FileSystems.getDefault().getPath(sourceFile.getAbsolutePath());
    final Path linkPath = FileSystems.getDefault().getPath(hardlink.getAbsolutePath());
    Files.createLink(linkPath, sourcePath);
    return hardlink;
  }

  private static File copyFile(File sourceFile, File targetFile) throws IOException {
    if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
      throw new IOException(
          String.format(
              "failed to copy file %s to %s: failed to create parent dir %s",
              sourceFile.getPath(), targetFile.getPath(), targetFile.getParentFile().getPath()));
    }

    Files.copy(sourceFile.toPath(), targetFile.toPath());
    return targetFile;
  }

  /**
   * given a hardlink or copied file, decrease its reference count, if the reference count is 0,
   * delete the file. if the given file is not a hardlink or copied file, do nothing.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @throws IOException when delete file failed
   */
  public synchronized void decreaseFileReference(File hardlinkOrCopiedFile) throws IOException {
    final Integer updatedReference =
        hardlinkOrCopiedFileToReferenceMap.computeIfPresent(
            hardlinkOrCopiedFile.getPath(), (file, reference) -> reference - 1);

    if (updatedReference != null && updatedReference == 0) {
      Files.deleteIfExists(hardlinkOrCopiedFile.toPath());
      hardlinkOrCopiedFileToReferenceMap.remove(hardlinkOrCopiedFile.getPath());
    }
  }

  /**
   * get the reference count of the file.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @return the reference count of the file
   */
  public synchronized int getFileReferenceCount(File hardlinkOrCopiedFile) {
    return hardlinkOrCopiedFileToReferenceMap.getOrDefault(hardlinkOrCopiedFile.getPath(), 0);
  }
}
