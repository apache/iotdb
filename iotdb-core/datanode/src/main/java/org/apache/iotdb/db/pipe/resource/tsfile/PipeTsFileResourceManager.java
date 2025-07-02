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
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceManager.class);

  // This is used to hold the assigner pinned tsFiles.
  // Also, it is used to provide metadata cache of the tsFile, and is shared by all the pipe's
  // tsFiles.
  private final Map<String, PipeTsFileMemResource> hardlinkOrCopiedFileToTsFileMemResourceMap =
      new ConcurrentHashMap<>();

  // PipeName -> TsFilePath -> PipeTsFileResource
  private final Map<String, Map<String, PipeTsFileResource>>
      hardlinkOrCopiedFileToPipeTsFileResourceMap = new ConcurrentHashMap<>();
  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

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
   * @return the hardlink or copied file
   * @throws IOException when create hardlink or copy file failed
   */
  public File increaseFileReference(
      final File file, final boolean isTsFile, final @Nonnull String pipeName) throws IOException {
    // If the file is already a hardlink or copied file,
    // just increase reference count and return it
    segmentLock.lock(file);
    try {
      if (increaseReferenceIfExists(file, isTsFile, pipeName)) {
        return file;
      }
    } finally {
      segmentLock.unlock(file);
    }

    // If the file is not a hardlink or copied file, check if there is a related hardlink or
    // copied file in pipe dir. if so, increase reference count and return it
    final File hardlinkOrCopiedFile = getHardlinkOrCopiedFileInPipeDir(file, pipeName);
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      if (increaseReferenceIfExists(hardlinkOrCopiedFile, isTsFile, pipeName)) {
        return hardlinkOrCopiedFileToPipeTsFileResourceMap
            .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
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
      hardlinkOrCopiedFileToPipeTsFileResourceMap
          .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
          .put(resultFile.getPath(), new PipeTsFileResource(resultFile));

      increaseMemReference(resultFile, isTsFile);

      return resultFile;
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
  }

  private boolean increaseReferenceIfExists(
      final File file, final boolean isTsFile, final @Nonnull String pipeName) {
    final String path = file.getPath();
    final PipeTsFileResource resource =
        hardlinkOrCopiedFileToPipeTsFileResourceMap
            .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
            .get(path);
    if (resource != null) {
      resource.increaseReferenceCount();
      increaseMemReference(file, isTsFile);
      return true;
    }
    return false;
  }

  private void increaseMemReference(final File file, final boolean isTsFile) {
    if (!isTsFile) {
      return;
    }
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    hardlinkOrCopiedFileToTsFileMemResourceMap.compute(
        getCommonFilePath(file),
        (k, v) -> {
          if (Objects.isNull(v)) {
            return new PipeTsFileMemResource();
          } else {
            v.increaseReferenceCount();
            return v;
          }
        });
  }

  public static File getHardlinkOrCopiedFileInPipeDir(final File file, final String pipeName)
      throws IOException {
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

  private static String getPipeTsFileDirPath(File file, final String pipeName) throws IOException {
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
      final File hardlinkOrCopiedFile, final @Nonnull String pipeName) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource =
          hardlinkOrCopiedFileToPipeTsFileResourceMap
              .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
              .get(filePath);
      if (resource != null && resource.decreaseReferenceCount()) {
        hardlinkOrCopiedFileToPipeTsFileResourceMap
            .computeIfAbsent(pipeName, pipe -> new ConcurrentHashMap<>())
            .remove(filePath);
      }
      // Decrease the assigner's file to clear hard-link and memory cache
      // Note that it does not exist for historical files
      decreaseMemReferenceIfExists(hardlinkOrCopiedFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
  }

  private void decreaseMemReferenceIfExists(final File file) {
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    final String commonFilePath = getCommonFilePath(file);
    if (hardlinkOrCopiedFileToTsFileMemResourceMap.containsKey(commonFilePath)
        && hardlinkOrCopiedFileToTsFileMemResourceMap
            .get(commonFilePath)
            .decreaseReferenceCount()) {
      hardlinkOrCopiedFileToPipeTsFileResourceMap.remove(commonFilePath);
    }
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
              : hardlinkOrCopiedFileToTsFileMemResourceMap.get(
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
      final PipeTsFileMemResource resource =
          hardlinkOrCopiedFileToTsFileMemResourceMap.get(getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource != null && resource.cacheObjectsIfAbsent(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, List<String>> getDeviceMeasurementsMapFromCache(
      final File hardlinkOrCopiedTsFile) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileMemResource resource =
          hardlinkOrCopiedFileToTsFileMemResourceMap.get(getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource == null ? null : resource.tryGetDeviceMeasurementsMap(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public Map<IDeviceID, Boolean> getDeviceIsAlignedMapFromCache(
      final File hardlinkOrCopiedTsFile, final boolean cacheOtherMetadata) throws IOException {
    segmentLock.lock(hardlinkOrCopiedTsFile);
    try {
      final PipeTsFileMemResource resource =
          hardlinkOrCopiedFileToTsFileMemResourceMap.get(getCommonFilePath(hardlinkOrCopiedTsFile));
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
      final PipeTsFileMemResource resource =
          hardlinkOrCopiedFileToTsFileMemResourceMap.get(getCommonFilePath(hardlinkOrCopiedTsFile));
      return resource == null
          ? null
          : resource.tryGetMeasurementDataTypeMap(hardlinkOrCopiedTsFile);
    } finally {
      segmentLock.unlock(hardlinkOrCopiedTsFile);
    }
  }

  public void pinTsFileResource(
      final TsFileResource resource, final boolean withMods, final String pipeName)
      throws IOException {
    increaseFileReference(resource.getTsFile(), true, pipeName);
    if (withMods && resource.getModFile().exists()) {
      increaseFileReference(new File(resource.getModFile().getFilePath()), false, pipeName);
    }
  }

  public void unpinTsFile(final File tsFile, final String pipeName) throws IOException {
    final File pinnedFile = getHardlinkOrCopiedFileInPipeDir(tsFile, pipeName);
    decreaseFileReference(pinnedFile, pipeName);

    final File modFile = new File(pinnedFile + ModificationFile.FILE_SUFFIX);
    if (modFile.exists()) {
      decreaseFileReference(modFile, pipeName);
    }
  }

  public Queue<File> recoverTsFile(final @Nonnull String pipeName) {
    final Queue<File> result = new ArrayDeque<>();
    final String suffix =
        File.separator
            + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
            + File.separator
            + PipeConfig.getInstance().getPipeHardlinkTsFileDirName()
            + File.separator
            + pipeName;
    for (final String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {
      final File pipeDir = SystemFileFactory.INSTANCE.getFile(dataDir + suffix);
      if (pipeDir.exists() && pipeDir.isDirectory()) {
        final File[] files = pipeDir.listFiles();
        for (final File file : files) {
          try {
            final boolean isTsFile = file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX);
            increaseFileReference(file, isTsFile, pipeName);
            if (isTsFile) {
              result.add(file);
            }
          } catch (final IOException e) {
            throw new PipeException(e.getMessage());
          }
        }
      } else {
        return null;
      }
    }
    return result;
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
