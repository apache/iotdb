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
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalJobExecutor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTsFileDataRegionResourceManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTsFileDataRegionResourceManager.class);

  private final Map<String, PipeTsFileResource> hardlinkOrCopiedFileToPipeTsFileResourceMap =
      new ConcurrentHashMap<>();
  private final ReentrantLock lock = new ReentrantLock();

  void tryTtlCheck() {
    try {
      final long timeout = PipePeriodicalJobExecutor.getMinIntervalSeconds() >> 1;
      if (lock.tryLock(timeout, TimeUnit.SECONDS)) {
        try {
          ttlCheck();
        } finally {
          lock.unlock();
        }
      } else {
        LOGGER.warn("failed to try lock when checking TTL because of timeout ({}s)", timeout);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("failed to try lock when checking TTL because of interruption", e);
    }
  }

  private void ttlCheck() {
    final Iterator<Map.Entry<String, PipeTsFileResource>> iterator =
        hardlinkOrCopiedFileToPipeTsFileResourceMap.entrySet().iterator();
    final Optional<Logger> logger =
        PipeDataNodeResourceManager.log()
            .schedule(
                PipeTsFileResourceManager.class,
                PipeConfig.getInstance().getPipeTsFilePinMaxLogNumPerRound(),
                PipeConfig.getInstance().getPipeTsFilePinMaxLogIntervalRounds(),
                hardlinkOrCopiedFileToPipeTsFileResourceMap.size());

    while (iterator.hasNext()) {
      final Map.Entry<String, PipeTsFileResource> entry = iterator.next();

      try {
        if (entry.getValue().closeIfOutOfTimeToLive()) {
          iterator.remove();
        } else {
          logger.ifPresent(
              l ->
                  l.info(
                      "Pipe file (file name: {}) is still referenced {} times",
                      entry.getKey(),
                      entry.getValue().getReferenceCount()));
        }
      } catch (final IOException e) {
        LOGGER.warn("failed to close PipeTsFileResource when checking TTL: ", e);
      }
    }
  }

  File increaseFileReference(
      final File file, final boolean isTsFile, final TsFileResource tsFileResource)
      throws IOException {
    lock.lock();
    try {
      // If the file is already a hardlink or copied file,
      // just increase reference count and return it
      if (increaseReferenceIfExists(file.getPath())) {
        return file;
      }

      // If the file is not a hardlink or copied file, check if there is a related hardlink or
      // copied file in pipe dir. if so, increase reference count and return it
      final File hardlinkOrCopiedFile = getHardlinkOrCopiedFileInPipeDir(file);
      if (increaseReferenceIfExists(hardlinkOrCopiedFile.getPath())) {
        return hardlinkOrCopiedFileToPipeTsFileResourceMap
            .get(hardlinkOrCopiedFile.getPath())
            .getFile();
      }

      // If the file is a tsfile, create a hardlink in pipe dir and will return it.
      // otherwise, copy the file (.mod or .resource) to pipe dir and will return it.
      final File resultFile =
          isTsFile
              ? FileUtils.createHardLink(file, hardlinkOrCopiedFile)
              : FileUtils.copyFile(file, hardlinkOrCopiedFile);
      // If the file is not a hardlink or copied file, and there is no related hardlink or copied
      // file in pipe dir, create a hardlink or copy it to pipe dir, maintain a reference count for
      // the hardlink or copied file, and return the hardlink or copied file.
      hardlinkOrCopiedFileToPipeTsFileResourceMap.put(
          resultFile.getPath(), new PipeTsFileResource(resultFile, isTsFile, tsFileResource));
      return resultFile;
    } finally {
      lock.unlock();
    }
  }

  private boolean increaseReferenceIfExists(final String path) {
    final PipeTsFileResource resource = hardlinkOrCopiedFileToPipeTsFileResourceMap.get(path);
    if (resource != null) {
      resource.increaseAndGetReference();
      return true;
    }
    return false;
  }

  public static File getHardlinkOrCopiedFileInPipeDir(final File file) throws IOException {
    try {
      return new File(getPipeTsFileDirPath(file), getRelativeFilePath(file));
    } catch (final Exception e) {
      throw new IOException(
          String.format(
              "failed to get hardlink or copied file in pipe dir "
                  + "for file %s, it is not a tsfile, mod file or resource file",
              file.getPath()),
          e);
    }
  }

  private static String getPipeTsFileDirPath(File file) throws IOException {
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)) {
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
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)) {
      file = file.getParentFile();
      builder =
          new StringBuilder(file.getName())
              .append(IoTDBConstant.FILE_NAME_SEPARATOR)
              .append(builder);
    }
    return builder.toString();
  }

  void decreaseFileReference(final File hardlinkOrCopiedFile) {
    lock.lock();
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = hardlinkOrCopiedFileToPipeTsFileResourceMap.get(filePath);
      if (resource != null) {
        resource.decreaseAndGetReference();
      }
    } finally {
      lock.unlock();
    }
  }

  public int getFileReferenceCount(final File hardlinkOrCopiedFile) {
    lock.lock();
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = hardlinkOrCopiedFileToPipeTsFileResourceMap.get(filePath);
      return resource != null ? resource.getReferenceCount() : 0;
    } finally {
      lock.unlock();
    }
  }

  boolean cacheObjectsIfAbsent(final File hardlinkOrCopiedTsFile) throws IOException {
    lock.lock();
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource != null && resource.cacheObjectsIfAbsent();
    } finally {
      lock.unlock();
    }
  }

  Map<IDeviceID, List<String>> getDeviceMeasurementsMapFromCache(final File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetDeviceMeasurementsMap();
    } finally {
      lock.unlock();
    }
  }

  Map<IDeviceID, Boolean> getDeviceIsAlignedMapFromCache(
      final File hardlinkOrCopiedTsFile, final boolean cacheOtherMetadata) throws IOException {
    lock.lock();
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetDeviceIsAlignedMap(cacheOtherMetadata);
    } finally {
      lock.unlock();
    }
  }

  Map<String, TSDataType> getMeasurementDataTypeMapFromCache(final File hardlinkOrCopiedTsFile)
      throws IOException {
    lock.lock();
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetMeasurementDataTypeMap();
    } finally {
      lock.unlock();
    }
  }

  void pinTsFileResource(final TsFileResource resource, final boolean withMods) throws IOException {
    lock.lock();
    try {
      increaseFileReference(resource.getTsFile(), true, resource);
      if (withMods && resource.getModFile().exists()) {
        increaseFileReference(new File(resource.getModFile().getFilePath()), false, null);
      }
    } finally {
      lock.unlock();
    }
  }

  void unpinTsFileResource(final TsFileResource resource) throws IOException {
    lock.lock();
    try {
      final File pinnedFile = getHardlinkOrCopiedFileInPipeDir(resource.getTsFile());
      decreaseFileReference(pinnedFile);

      final File modFile = new File(pinnedFile + ModificationFile.FILE_SUFFIX);
      if (modFile.exists()) {
        decreaseFileReference(modFile);
      }
    } finally {
      lock.unlock();
    }
  }

  int getLinkedTsfileCount() {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToPipeTsFileResourceMap.size();
    } finally {
      lock.unlock();
    }
  }

  long getTotalLinkedButDeletedTsfileSize() {
    lock.lock();
    try {
      return hardlinkOrCopiedFileToPipeTsFileResourceMap.values().parallelStream()
          .filter(PipeTsFileResource::isOriginalTsFileDeleted)
          .mapToLong(
              resource -> {
                try {
                  return resource.getFileSize();
                } catch (Exception e) {
                  LOGGER.warn(
                      "failed to get file size of linked but deleted TsFile {}: ", resource, e);
                  return 0;
                }
              })
          .sum();
    } finally {
      lock.unlock();
    }
  }
}
