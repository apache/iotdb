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
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
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

public class PipeTsFileResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceManager.class);

  private final Map<String, PipeTsFileResource> hardlinkOrCopiedFileToPipeTsFileResourceMap =
      new ConcurrentHashMap<>();
  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

  public PipeTsFileResourceManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeTsFileResourceManager#ttlCheck()",
            this::tryTtlCheck,
            Math.max(PipeTsFileResource.TSFILE_MIN_TIME_TO_LIVE_IN_MS / 1000, 1));
  }

  private void tryTtlCheck() {
    try {
      ttlCheck();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("failed to try lock when checking TTL because of interruption", e);
    } catch (final Exception e) {
      LOGGER.warn("failed to check TTL of PipeTsFileResource: ", e);
    }
  }

  private void ttlCheck() throws InterruptedException {
    final Iterator<Map.Entry<String, PipeTsFileResource>> iterator =
        hardlinkOrCopiedFileToPipeTsFileResourceMap.entrySet().iterator();
    final long timeout =
        PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds() >> 1;
    final Optional<Logger> logger =
        PipeDataNodeResourceManager.log()
            .schedule(
                PipeTsFileResourceManager.class,
                PipeConfig.getInstance().getPipeTsFilePinMaxLogNumPerRound(),
                PipeConfig.getInstance().getPipeTsFilePinMaxLogIntervalRounds(),
                hardlinkOrCopiedFileToPipeTsFileResourceMap.size());
    final StringBuilder logBuilder = new StringBuilder();
    while (iterator.hasNext()) {
      final Map.Entry<String, PipeTsFileResource> entry = iterator.next();

      final String hardlinkOrCopiedFile = entry.getKey();
      if (!segmentLock.tryLock(new File(hardlinkOrCopiedFile), timeout, TimeUnit.SECONDS)) {
        LOGGER.warn(
            "failed to try lock when checking TTL for file {} because of timeout ({}s)",
            hardlinkOrCopiedFile,
            timeout);
        continue;
      }

      try {
        if (entry.getValue().closeIfOutOfTimeToLive()) {
          iterator.remove();
        } else {
          logBuilder.append(
              String.format(
                  "<%s , %d times> ", entry.getKey(), entry.getValue().getReferenceCount()));
        }
      } catch (final IOException e) {
        LOGGER.warn("failed to close PipeTsFileResource when checking TTL: ", e);
      } finally {
        segmentLock.unlock(new File(hardlinkOrCopiedFile));
      }
    }
    if (logBuilder.length() > 0) {
      logger.ifPresent(l -> l.info("Pipe file {}are still referenced", logBuilder));
    }
  }

  /**
   * Given a file, create a hardlink or copy it to pipe dir, maintain a reference count for the
   * hardlink or copied file, and return the hardlink or copied file.
   *
   * <p>If the given file is already a hardlink or copied file, increase its reference count and
   * return it.
   *
   * <p>If the given file is a tsfile, create a hardlink in pipe dir, increase the reference count
   * of the hardlink and return it.
   *
   * <p>Otherwise, copy the file (.mod or .resource) to pipe dir, increase the reference count of
   * the copied file and return it.
   *
   * @param file tsfile, resource file or mod file. can be original file or hardlink/copy of
   *     original file
   * @param isTsFile {@code true} to create hardlink, {@code false} to copy file
   * @param tsFileResource the TsFileResource of original TsFile. Ignored if {@param isTsFile} is
   *     {@code false}.
   * @return the hardlink or copied file
   * @throws IOException when create hardlink or copy file failed
   */
  public File increaseFileReference(
      final File file, final boolean isTsFile, final TsFileResource tsFileResource)
      throws IOException {
    // If the file is already a hardlink or copied file,
    // just increase reference count and return it
    segmentLock.lock(file);
    try {
      if (increaseReferenceIfExists(file)) {
        return file;
      }
    } finally {
      segmentLock.unlock(file);
    }

    // If the file is not a hardlink or copied file, check if there is a related hardlink or
    // copied file in pipe dir. if so, increase reference count and return it
    final File hardlinkOrCopiedFile = getHardlinkOrCopiedFileInPipeDir(file);
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      if (increaseReferenceIfExists(hardlinkOrCopiedFile)) {
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
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
  }

  private boolean increaseReferenceIfExists(final File file) {
    final PipeTsFileResource resource =
        hardlinkOrCopiedFileToPipeTsFileResourceMap.get(file.getPath());
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

  /**
   * Given a hardlink or copied file, decrease its reference count, if the reference count is 0,
   * delete the file. if the given file is not a hardlink or copied file, do nothing.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   */
  public void decreaseFileReference(final File hardlinkOrCopiedFile) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = hardlinkOrCopiedFileToPipeTsFileResourceMap.get(filePath);
      if (resource != null) {
        resource.decreaseAndGetReference();
      }
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
  }

  /**
   * Get the reference count of the file.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @return the reference count of the file
   */
  public int getFileReferenceCount(final File hardlinkOrCopiedFile) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = hardlinkOrCopiedFileToPipeTsFileResourceMap.get(filePath);
      return resource != null ? resource.getReferenceCount() : 0;
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
  }

  /**
   * Cache maps of the TsFile for further use.
   *
   * @return {@code true} if the maps are successfully put into cache or already cached. {@code
   *     false} if they can not be cached.
   */
  public boolean cacheObjectsIfAbsent(final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource != null && resource.cacheObjectsIfAbsent();
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, List<String>> getDeviceMeasurementsMapFromCache(
      final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetDeviceMeasurementsMap();
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, Boolean> getDeviceIsAlignedMapFromCache(
      final File hardlinkOrCopiedTsFile, final boolean cacheOtherMetadata) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetDeviceIsAlignedMap(cacheOtherMetadata);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<String, TSDataType> getMeasurementDataTypeMapFromCache(
      final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap.get(hardlinkOrCopiedTsFile.getPath());
      return resource == null ? null : resource.tryGetMeasurementDataTypeMap();
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public void pinTsFileResource(final TsFileResource resource, final boolean withMods)
      throws IOException {
    increaseFileReference(resource.getTsFile(), true, resource);
    if (withMods && resource.getExclusiveModFile().exists()) {
      increaseFileReference(resource.getExclusiveModFile().getFile(), false, null);
    }
  }

  public void unpinTsFileResource(final TsFileResource resource) throws IOException {
    final File pinnedFile = getHardlinkOrCopiedFileInPipeDir(resource.getTsFile());
    decreaseFileReference(pinnedFile);

    if (resource.sharedModFileExists()) {
      decreaseFileReference(resource.getSharedModFile().getFile());
    }
  }

  public int getLinkedTsfileCount() {
    return hardlinkOrCopiedFileToPipeTsFileResourceMap.size();
  }

  /**
   * Get the total size of linked TsFiles whose original TsFile is deleted (by compaction or else)
   */
  public long getTotalLinkedButDeletedTsfileSize() {
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
    } catch (final Exception e) {
      LOGGER.warn("failed to get total size of linked but deleted TsFiles: ", e);
      return 0;
    }
  }

  public long getTotalLinkedButDeletedTsfileResourceRamSize() {
    long totalLinkedButDeletedTsfileResourceRamSize = 0;
    try {
      for (final Map.Entry<String, PipeTsFileResource> resourceEntry :
          hardlinkOrCopiedFileToPipeTsFileResourceMap.entrySet()) {
        final PipeTsFileResource pipeTsFileResource = resourceEntry.getValue();
        // If the original TsFile is not deleted, the memory of the resource is not counted
        // because the memory of the resource is controlled by TsFileResourceManager.
        if (pipeTsFileResource.isOriginalTsFileDeleted()) {
          totalLinkedButDeletedTsfileResourceRamSize += pipeTsFileResource.getTsFileResourceSize();
        }
      }
      return totalLinkedButDeletedTsfileResourceRamSize;
    } catch (final Exception e) {
      LOGGER.warn("failed to get total size of linked but deleted TsFiles resource ram size: ", e);
      return totalLinkedButDeletedTsfileResourceRamSize;
    }
  }
}
