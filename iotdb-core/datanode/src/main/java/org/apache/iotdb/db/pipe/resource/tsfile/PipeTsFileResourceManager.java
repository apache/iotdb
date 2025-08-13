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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceManager.class);

  // This is used to hold the assigner pinned tsFiles.
  // Also, it is used to provide metadata cache of the tsFile, and is shared by all the pipe's
  // tsFiles.
  private final Map<String, PipeTsFilePublicResource>
      hardlinkOrCopiedFileToTsFilePublicResourceMap = new ConcurrentHashMap<>();

  // PipeName -> TsFilePath -> PipeTsFileResource
  private final Map<String, Map<String, PipeTsFileResource>>
      hardlinkOrCopiedFileToPipeTsFileResourceMap = new ConcurrentHashMap<>();
  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

  public File increaseFileReference(
      final File file, final boolean isTsFile, final @Nullable String pipeName) throws IOException {
    return increaseFileReference(file, isTsFile, pipeName, null);
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
   * @param pipeName Nonnull if the pipe is from historical or assigner -> extractors, null if is
   *     dataRegion -> assigner
   * @param sourceFile for inner use, historical extractor will use this to create hardlink from
   *     pipe tsFile -> common tsFile
   * @return the hardlink or copied file
   * @throws IOException when create hardlink or copy file failed
   */
  private File increaseFileReference(
      final File file,
      final boolean isTsFile,
      final @Nullable String pipeName,
      final @Nullable File sourceFile)
      throws IOException {
    // If the file is already a hardlink or copied file,
    // just increase reference count and return it
    if (increaseReferenceIfExists(file, pipeName, isTsFile)) {
      return file;
    }

    // If the file is not a hardlink or copied file, check if there is a related hardlink or
    // copied file in pipe dir. if so, increase reference count and return it
    final File hardlinkOrCopiedFile =
        Objects.isNull(sourceFile) ? getHardlinkOrCopiedFileInPipeDir(file, pipeName) : file;

    if (increaseReferenceIfExists(hardlinkOrCopiedFile, pipeName, isTsFile)) {
      return getResourceMap(pipeName).get(hardlinkOrCopiedFile.getPath()).getFile();
    }

    // If the file is a tsfile, create a hardlink in pipe dir and will return it.
    // otherwise, copy the file (.mod or .resource) to pipe dir and will return it.
    final File source = Objects.isNull(sourceFile) ? file : sourceFile;
    final File resultFile;

    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      resultFile =
          isTsFile
              ? FileUtils.createHardLink(source, hardlinkOrCopiedFile)
              : FileUtils.copyFile(source, hardlinkOrCopiedFile);

      // If the file is not a hardlink or copied file, and there is no related hardlink or copied
      // file in pipe dir, create a hardlink or copy it to pipe dir, maintain a reference count for
      // the hardlink or copied file, and return the hardlink or copied file.
      if (Objects.nonNull(pipeName)) {
        hardlinkOrCopiedFileToPipeTsFileResourceMap
            .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
            .put(resultFile.getPath(), new PipeTsFileResource(resultFile));
      } else {
        hardlinkOrCopiedFileToTsFilePublicResourceMap.put(
            resultFile.getPath(), new PipeTsFilePublicResource(resultFile));
      }
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
    increasePublicReference(resultFile, pipeName, isTsFile);
    return resultFile;
  }

  private boolean increaseReferenceIfExists(
      final File file, final @Nullable String pipeName, final boolean isTsFile) throws IOException {
    segmentLock.lock(file);
    try {
      final String path = file.getPath();
      final PipeTsFileResource resource = getResourceMap(pipeName).get(path);
      if (resource != null) {
        resource.increaseReferenceCount();
      } else {
        return false;
      }
    } finally {
      segmentLock.unlock(file);
    }
    increasePublicReference(file, pipeName, isTsFile);
    return true;
  }

  private void increasePublicReference(
      final File file, final @Nullable String pipeName, final boolean isTsFile) throws IOException {
    if (Objects.isNull(pipeName)) {
      return;
    }
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    increaseFileReference(new File(getCommonFilePath(file)), isTsFile, null, file);
  }

  public static File getHardlinkOrCopiedFileInPipeDir(
      final File file, final @Nullable String pipeName) throws IOException {
    try {
      return new File(getPipeTsFileDirPath(file, pipeName), getRelativeFilePath(file));
    } catch (final Exception e) {
      throw new IOException(
          String.format(
              "failed to get hardlink or copied file in pipe dir "
                  + "for file %s, it is not a tsfile, mod file or resource file",
              file.getPath()),
          e);
    }
  }

  private static String getPipeTsFileDirPath(File file, final @Nullable String pipeName)
      throws IOException {
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
        && !file.getName().equals(PipeConfig.getInstance().getPipeHardlinkBaseDirName())) {
      file = file.getParentFile();
    }
    return file.getParentFile().getCanonicalPath()
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
        + File.separator
        + PipeConfig.getInstance().getPipeHardlinkTsFileDirName()
        + (Objects.nonNull(pipeName) ? File.separator + pipeName : "");
  }

  private static String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
        && !file.getParentFile()
            .getName()
            .equals(PipeConfig.getInstance().getPipeHardlinkTsFileDirName())) {
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
   * @param hardlinkOrCopiedFile the copied or hard-linked file
   */
  public void decreaseFileReference(
      final File hardlinkOrCopiedFile, final @Nullable String pipeName) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = getResourceMap(pipeName).get(filePath);
      if (resource != null && resource.decreaseReferenceCount()) {
        getResourceMap(pipeName).remove(filePath);
      }
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }

    // Decrease the assigner's file to clear hard-link and memory cache
    // Note that it does not exist for historical files
    decreasePublicReferenceIfExists(hardlinkOrCopiedFile, pipeName);
  }

  private void decreasePublicReferenceIfExists(final File file, final @Nullable String pipeName) {
    if (Objects.isNull(pipeName)) {
      return;
    }
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    decreaseFileReference(new File(getCommonFilePath(file)), null);
  }

  // Warning: Shall not be called by the assigner
  private String getCommonFilePath(final @Nonnull File file) {
    // If the parent or grandparent is null then this is testing scenario
    // Skip the "pipeName" of this file
    return Objects.isNull(file.getParentFile())
            || Objects.isNull(file.getParentFile().getParentFile())
        ? file.getPath()
        : file.getParentFile().getParent() + File.separator + file.getName();
  }

  /**
   * Get the reference count of the file.
   *
   * @param hardlinkOrCopiedFile the copied or hardlinked file
   * @return the reference count of the file
   */
  @TestOnly
  public int getFileReferenceCount(
      final File hardlinkOrCopiedFile, final @Nullable String pipeName) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final PipeTsFileResource resource =
          Objects.nonNull(pipeName)
              ? hardlinkOrCopiedFileToPipeTsFileResourceMap
                  .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
                  .get(hardlinkOrCopiedFile.getPath())
              : hardlinkOrCopiedFileToTsFilePublicResourceMap.get(
                  getCommonFilePath(hardlinkOrCopiedFile));
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
      if (hardlinkOrCopiedTsFile.getParentFile() == null
          || hardlinkOrCopiedTsFile.getParentFile().getParentFile() == null) {
        return false;
      }
      final PipeTsFilePublicResource resource =
          hardlinkOrCopiedFileToTsFilePublicResourceMap.get(
              getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource != null && resource.cacheObjectsIfAbsent(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, List<String>> getDeviceMeasurementsMapFromCache(
      final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFilePublicResource resource =
          hardlinkOrCopiedFileToTsFilePublicResourceMap.get(
              getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource == null ? null : resource.tryGetDeviceMeasurementsMap(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, Boolean> getDeviceIsAlignedMapFromCache(
      final File hardlinkOrCopiedTsFile, final boolean cacheOtherMetadata) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFilePublicResource resource =
          hardlinkOrCopiedFileToTsFilePublicResourceMap.get(
              getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource == null
          ? null
          : resource.tryGetDeviceIsAlignedMap(cacheOtherMetadata, hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<String, TSDataType> getMeasurementDataTypeMapFromCache(
      final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFilePublicResource resource =
          hardlinkOrCopiedFileToTsFilePublicResourceMap.get(
              getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource == null
          ? null
          : resource.tryGetMeasurementDataTypeMap(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<String, ? extends PipeTsFileResource> getResourceMap(final @Nullable String pipeName) {
    return Objects.nonNull(pipeName)
        ? hardlinkOrCopiedFileToPipeTsFileResourceMap.computeIfAbsent(
            pipeName, k -> new ConcurrentHashMap<>())
        : hardlinkOrCopiedFileToTsFilePublicResourceMap;
  }

  public void pinTsFileResource(
      final TsFileResource resource, final boolean withMods, final @Nullable String pipeName)
      throws IOException {
    increaseFileReference(resource.getTsFile(), true, pipeName);
    if (withMods && resource.getExclusiveModFile().exists()) {
      increaseFileReference(resource.getExclusiveModFile().getFile(), false, pipeName);
    }
  }

  public void unpinTsFileResource(final TsFileResource resource, final @Nullable String pipeName)
      throws IOException {
    final File pinnedFile = getHardlinkOrCopiedFileInPipeDir(resource.getTsFile(), pipeName);
    decreaseFileReference(pinnedFile, pipeName);

    if (resource.sharedModFileExists()) {
      decreaseFileReference(resource.getSharedModFile().getFile(), pipeName);
    }
  }

  public int getLinkedTsFileCount(final @Nonnull String pipeName) {
    return hardlinkOrCopiedFileToPipeTsFileResourceMap
        .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
        .size();
  }

  public long getTotalLinkedTsFileSize(final @Nonnull String pipeName) {
    return hardlinkOrCopiedFileToPipeTsFileResourceMap
        .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
        .values()
        .stream()
        .mapToLong(
            resource -> {
              try {
                return resource.getFileSize();
              } catch (Exception e) {
                LOGGER.warn("failed to get file size of linked TsFile {}: ", resource, e);
                return 0;
              }
            })
        .sum();
  }
}
