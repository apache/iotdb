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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTsFileResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceManager.class);

  private final Map<String, PipeTsFileResource> hardlinkOrCopiedFileToTsFileResourceMap =
      new HashMap<>();
  private final ReentrantLock lock = new ReentrantLock();

  private static final ScheduledExecutorService PIPE_TSFILE_RESOURCE_TTL_CHECKER =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_TSFILE_RESOURCE_TTL_CHECKER.getName());

  public PipeTsFileResourceManager() {
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        PIPE_TSFILE_RESOURCE_TTL_CHECKER,
        () -> {
          final Iterator<Map.Entry<String, PipeTsFileResource>> iterator =
              hardlinkOrCopiedFileToTsFileResourceMap.entrySet().iterator();
          while (iterator.hasNext()) {
            final Map.Entry<String, PipeTsFileResource> entry = iterator.next();

            lock.lock();
            try {
              if (entry.getValue().closeIfOutOfTimeToLive()) {
                iterator.remove();
              } else {
                LOGGER.info(
                    "Pipe file (file name: {}) is still referenced {} times",
                    entry.getKey(),
                    entry.getValue().getReferenceCount());
              }
            } catch (IOException e) {
              LOGGER.warn("failed to close PipeTsFileResource when checking TTL: ", e);
            } finally {
              lock.unlock();
            }
          }
        },
        PipeTsFileResource.TSFILE_MIN_TIME_TO_LIVE_IN_MS,
        PipeTsFileResource.TSFILE_MIN_TIME_TO_LIVE_IN_MS,
        TimeUnit.MILLISECONDS);
  }

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
  public File increaseFileReference(File file, boolean isTsFile) throws IOException {
    lock.lock();
    try {
      // if the file is already a hardlink or copied file, just increase reference count and return
      // it
      if (increaseReferenceIfExists(file.getPath())) {
        return file;
      }

      // if the file is not a hardlink or copied file, check if there is a related hardlink or
      // copied
      // file in pipe dir. if so, increase reference count and return it
      final File hardlinkOrCopiedFile = getHardlinkOrCopiedFileInPipeDir(file);
      if (increaseReferenceIfExists(hardlinkOrCopiedFile.getPath())) {
        return hardlinkOrCopiedFileToTsFileResourceMap
            .get(hardlinkOrCopiedFile.getPath())
            .getFile();
      }

      // if the file is a tsfile, create a hardlink in pipe dir and will return it.
      // otherwise, copy the file (.mod or .resource) to pipe dir and will return it.
      final File resultFile =
          isTsFile
              ? createHardLink(file, hardlinkOrCopiedFile)
              : copyFile(file, hardlinkOrCopiedFile);
      // if the file is not a hardlink or copied file, and there is no related hardlink or copied
      // file in pipe dir, create a hardlink or copy it to pipe dir, maintain a reference count for
      // the hardlink or copied file, and return the hardlink or copied file.
      hardlinkOrCopiedFileToTsFileResourceMap.put(
          resultFile.getPath(), new PipeTsFileResource(resultFile, isTsFile));
      return resultFile;
    } finally {
      lock.unlock();
    }
  }

  private boolean increaseReferenceIfExists(String path) {
    PipeTsFileResource resource = hardlinkOrCopiedFileToTsFileResourceMap.get(path);
    if (resource != null) {
      resource.increaseAndGetReference();
      return true;
    }
    return false;
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
  public void decreaseFileReference(File hardlinkOrCopiedFile) {
    lock.lock();
    try {
      String filePath = hardlinkOrCopiedFile.getPath();
      PipeTsFileResource resource = hardlinkOrCopiedFileToTsFileResourceMap.get(filePath);
      if (resource != null) {
        resource.decreaseAndGetReference();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * get the reference count of the file.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @return the reference count of the file
   */
  public int getFileReferenceCount(File hardlinkOrCopiedFile) {
    lock.lock();
    try {
      String filePath = hardlinkOrCopiedFile.getPath();
      PipeTsFileResource resource = hardlinkOrCopiedFileToTsFileResourceMap.get(filePath);
      return resource != null ? resource.getReferenceCount() : 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * get the cached {@link TsFileSequenceReader} of the tsfile.
   *
   * @param hardlinkOrCopiedTsFile the copied or hard-linked tsfile
   * @return the {@link TsFileSequenceReader} of the file, or {@code null} if the {@link
   *     TsFileSequenceReader} is not cached.
   */
  public TsFileSequenceReader getTsFileSequenceReaderFromCache(File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToTsFileResourceMap
          .get(hardlinkOrCopiedTsFile.getPath())
          .getTsFileSequenceReader();
    } finally {
      lock.unlock();
    }
  }

  /**
   * get the cached {@link TsFileReader} of the file.
   *
   * @param hardlinkOrCopiedTsFile the copied or hardlinked file
   * @return the {@link TsFileReader} of the file, or {@code null} if the {@link TsFileReader} is
   *     not cached.
   */
  public TsFileReader getTsFileReaderFromCache(File hardlinkOrCopiedTsFile) throws IOException {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToTsFileResourceMap
          .get(hardlinkOrCopiedTsFile.getPath())
          .getTsFileReader();
    } finally {
      lock.unlock();
    }
  }

  public Map<String, List<String>> getDeviceMeasurementsMapFromCache(File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToTsFileResourceMap
          .get(hardlinkOrCopiedTsFile.getPath())
          .getDeviceMeasurementsMap();
    } finally {
      lock.unlock();
    }
  }

  public Map<String, Boolean> getDeviceIsAlignedMapFromCache(File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToTsFileResourceMap
          .get(hardlinkOrCopiedTsFile.getPath())
          .getDeviceIsAlignedMap();
    } finally {
      lock.unlock();
    }
  }

  public Map<String, TSDataType> getMeasurementDataTypeMapFromCache(File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToTsFileResourceMap
          .get(hardlinkOrCopiedTsFile.getPath())
          .getMeasurementDataTypeMap();
    } finally {
      lock.unlock();
    }
  }

  public void pinTsFileResource(TsFileResource resource) throws IOException {
    lock.lock();
    try {
      increaseFileReference(resource.getTsFile(), true);
    } finally {
      lock.unlock();
    }
  }

  public void unpinTsFileResource(TsFileResource resource) throws IOException {
    lock.lock();
    try {
      decreaseFileReference(getHardlinkOrCopiedFileInPipeDir(resource.getTsFile()));
    } finally {
      lock.unlock();
    }
  }
}
