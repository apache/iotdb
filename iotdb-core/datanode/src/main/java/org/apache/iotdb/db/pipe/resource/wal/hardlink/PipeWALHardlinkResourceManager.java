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

package org.apache.iotdb.db.pipe.resource.wal.hardlink;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.resource.wal.PipeWALResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class PipeWALHardlinkResourceManager extends PipeWALResourceManager {

  @Override
  protected void pinInternal(final long memTableId, final WALEntryHandler walEntryHandler) {
    memtableIdToPipeWALResourceMap
        .computeIfAbsent(memTableId, id -> new PipeWALHardlinkResource(walEntryHandler, this))
        .pin();
  }

  @Override
  protected void unpinInternal(final long memTableId, final WALEntryHandler walEntryHandler) {
    memtableIdToPipeWALResourceMap.get(memTableId).unpin();
  }

  //////////////////////////// hardlink related ////////////////////////////

  private final Map<String, Integer> hardlinkToReferenceMap = new HashMap<>();

  /**
   * given a file, create a hardlink, maintain a reference count for the hardlink, and return the
   * hardlink.
   *
   * <p>if the given file is already a hardlink, increase its reference count and return it.
   *
   * <p>if the given file is a wal, create a hardlink in pipe dir, increase the reference count of
   * the hardlink and return it.
   *
   * @param file wal file. can be original file or the hardlink of original file
   * @return the hardlink
   * @throws IOException when create hardlink failed
   */
  public synchronized File increaseFileReference(final File file) throws IOException {
    // if the file is already a hardlink, just increase reference count and return it
    if (increaseReferenceIfExists(file.getPath())) {
      return file;
    }

    // if the file is not a hardlink, check if there is a related hardlink in pipe dir. if so,
    // increase reference count and return it.
    final File hardlink = getHardlinkInPipeWALDir(file);
    if (increaseReferenceIfExists(hardlink.getPath())) {
      return hardlink;
    }

    // if the file is a wal, and there is no related hardlink in pipe dir, create a hardlink to pipe
    // dir, maintain a reference count for the hardlink, and return the hardlink.
    hardlinkToReferenceMap.put(hardlink.getPath(), 1);
    return FileUtils.createHardLink(file, hardlink);
  }

  private boolean increaseReferenceIfExists(final String path) {
    hardlinkToReferenceMap.computeIfPresent(path, (k, v) -> v + 1);
    return hardlinkToReferenceMap.containsKey(path);
  }

  // TODO: Check me! Make sure the file is not a hardlink.
  // TODO: IF user specify a wal by config, will the method work?
  private static File getHardlinkInPipeWALDir(final File file) throws IOException {
    try {
      return new File(getPipeWALDirPath(file), getRelativeFilePath(file));
    } catch (final Exception e) {
      throw new IOException(
          String.format(
              "failed to get hardlink in pipe dir " + "for file %s, it is not a wal",
              file.getPath()),
          e);
    }
  }

  private static String getPipeWALDirPath(File file) throws IOException {
    while (!file.getName().equals(IoTDBConstant.WAL_FOLDER_NAME)) {
      file = file.getParentFile();
    }

    return file.getParentFile().getCanonicalPath()
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkWALDirName();
  }

  private static String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getParentFile().getName().equals(IoTDBConstant.WAL_FOLDER_NAME)) {
      file = file.getParentFile();
      builder =
          new StringBuilder(file.getName())
              .append(IoTDBConstant.FILE_NAME_SEPARATOR)
              .append(builder);
    }
    return builder.toString();
  }

  /**
   * given a hardlink, decrease its reference count, if the reference count is 0, delete the file.
   * if the given file is not a hardlink, do nothing.
   *
   * @param hardlink the hardlinked file
   * @throws IOException when delete file failed
   */
  public synchronized void decreaseFileReference(final File hardlink) throws IOException {
    final Integer updatedReference =
        hardlinkToReferenceMap.computeIfPresent(
            hardlink.getPath(), (file, reference) -> reference - 1);

    if (updatedReference != null && updatedReference == 0) {
      Files.deleteIfExists(hardlink.toPath());
      hardlinkToReferenceMap.remove(hardlink.getPath());
    }
  }

  @TestOnly
  public synchronized int getFileReferenceCount(final File hardlink) {
    return hardlinkToReferenceMap.getOrDefault(hardlink.getPath(), 0);
  }
}
