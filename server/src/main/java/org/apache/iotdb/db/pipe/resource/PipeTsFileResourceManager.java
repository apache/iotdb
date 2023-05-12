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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.config.PipeConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class PipeTsFileResourceManager {

  private final HashMap<String, Integer> tsFileReferenceMap = new HashMap<>();

  /**
   * increase the reference count of the file, if the file is a hardlink, just increase reference
   *
   * @param file the file must be in sequence, unsequence or pipe-tsfile dir
   * @param needHardlink whether to create a hardlink or copy the file
   * @return the file in pipe dir
   * @throws IOException when create hardlink or copy file failed
   */
  public synchronized File increaseFileReference(File file, boolean needHardlink)
      throws IOException {

    // if the file is a hardlink, just increase reference count and return it
    if (increaseReferenceIfExists(file.getPath())) {
      return file;
    }

    // if the hardlink of file is already exist, just increase reference count and return it
    File link = getFileInPipeDir(file);
    if (increaseReferenceIfExists(link.getPath())) {
      return link;
    }

    // if the file is a tsfile, create a hardlink in pipe dir and return it
    // otherwise, copy the file (.mod or .resource) to pipe dir and return it
    tsFileReferenceMap.put(link.getPath(), 1);
    return needHardlink ? createHardLink(file, link) : copyFile(file, link);
  }

  /**
   * decrease the reference count of the file, if the reference count is 0, delete the file
   *
   * @param file the copied or hardlinked file
   * @throws IOException when delete file failed
   */
  public synchronized void decreaseFileReference(File file) throws IOException {
    Integer updatedValue = tsFileReferenceMap.computeIfPresent(file.getPath(), (k, v) -> v - 1);
    if (updatedValue != null && updatedValue == 0) {
      Files.deleteIfExists(file.toPath());
      tsFileReferenceMap.remove(file.getPath());
    }
  }

  private boolean increaseReferenceIfExists(String path) {
    if (tsFileReferenceMap.containsKey(path)) {
      tsFileReferenceMap.put(path, tsFileReferenceMap.get(path) + 1);
      return true;
    }
    return false;
  }

  private File createHardLink(File sourceFile, File hardlink) throws IOException {
    if (!hardlink.getParentFile().exists()) {
      boolean ignored = hardlink.getParentFile().mkdirs();
    }
    Path sourcePath = FileSystems.getDefault().getPath(sourceFile.getAbsolutePath());
    Path linkPath = FileSystems.getDefault().getPath(hardlink.getAbsolutePath());
    Files.createLink(linkPath, sourcePath);
    return hardlink;
  }

  private File copyFile(File sourceFile, File targetFile) throws IOException {
    if (!targetFile.getParentFile().exists()) {
      boolean ignored = targetFile.getParentFile().mkdirs();
    }
    Files.copy(sourceFile.toPath(), targetFile.toPath());
    return targetFile;
  }

  private String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)
        && !file.getName().equals(PipeConfig.PIPE_TSFILE_DIR_NAME)) {
      file = file.getParentFile();
      builder =
          new StringBuilder(file.getName())
              .append(IoTDBConstant.FILE_NAME_SEPARATOR)
              .append(builder);
    }
    return builder.toString();
  }

  private String getPipeTsFileDirPath(File file) throws IOException {
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)
        && !file.getName().equals(PipeConfig.PIPE_TSFILE_DIR_NAME)) {
      file = file.getParentFile();
    }
    return file.getParentFile().getCanonicalPath()
        + File.separator
        + PipeConfig.PIPE_TSFILE_DIR_NAME;
  }

  private File getFileInPipeDir(File file) throws IOException {
    return new File(getPipeTsFileDirPath(file), getRelativeFilePath(file));
  }

  public int getFileReferenceCount(File tsFile) {
    return tsFileReferenceMap.getOrDefault(tsFile.getPath(), 0);
  }

  public void clear() {
    for (String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      File pipeTsFileDir = new File(dataDir, PipeConfig.PIPE_TSFILE_DIR_NAME);
      if (pipeTsFileDir.exists()) {
        FileUtils.deleteDirectory(pipeTsFileDir);
      }
    }
    tsFileReferenceMap.clear();
  }
}
