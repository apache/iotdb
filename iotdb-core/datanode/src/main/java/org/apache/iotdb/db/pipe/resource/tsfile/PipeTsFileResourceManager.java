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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTsFileResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceManager.class);

  private static final String UNFINISHED_FILE_SUFFIX = ".unfinished";

  // This is used to hold the assigner pinned tsFiles.
  // Also, it is used to provide metadata cache of the tsFile, and is shared by all the pipe's
  // tsFiles.
  private final Map<String, PipeTsFilePublicResource>
      hardlinkOrCopiedFileToTsFilePublicResourceMap = new ConcurrentHashMap<>();

  // PipeName -> TsFilePath -> PipeTsFileResource
  private final Map<String, Map<String, PipeTsFileResource>>
      hardlinkOrCopiedFileToPipeTsFileResourceMap = new ConcurrentHashMap<>();
  // PipeName -> the pipe directories that hold the pipe's files, a pipe may span multiple disks
  private final Map<String, Set<String>> pipeNameToPipeTsFileDirPathMap = new ConcurrentHashMap<>();
  private final Set<String> pipeTsFileResourcePipeNameSetUnderDeletion =
      ConcurrentHashMap.newKeySet();
  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

  public static String getPipeTsFileResourcePipeName(
      final @Nullable String pipeName, final long creationTime) {
    return Objects.isNull(pipeName) ? null : pipeName + "_" + creationTime;
  }

  public File increaseFileReference(
      final File file, final boolean isTsFile, final @Nullable String pipeName) throws IOException {
    return increaseFileReference(file, isTsFile, pipeName, null, null);
  }

  /**
   * Returns the hardlink or copied file of {@code file} in the pipe directory of {@code pipeName},
   * creating it and increasing its reference count if it does not exist yet. A {@code null} pipe
   * name means the public pipe file shared by the dataRegion -> assigner pipeline.
   *
   * <p>TsFiles are hardlinked, or copied if the hardlink can not be created, while .resource and
   * .mod files are always copied.
   *
   * @param sourceFile the file to hardlink or copy, or {@code null} to use {@code file}
   * @param resource the source resource of {@code file}, or {@code null} if it is already final
   * @throws IOException if the hardlink or the copy can not be created
   */
  private File increaseFileReference(
      final File file,
      final boolean isTsFile,
      final @Nullable String pipeName,
      final @Nullable File sourceFile,
      final @Nullable TsFileResource resource)
      throws IOException {
    // The file is already a pipe file, so only its reference count has to be increased.
    if (increaseReferenceIfExists(file, pipeName, isTsFile)) {
      return file;
    }

    // The pipe file may exist without being tracked, e.g. left by a previous process.
    final File hardlinkOrCopiedFile =
        Objects.isNull(sourceFile) ? getHardlinkOrCopiedFileInPipeDir(file, pipeName) : file;

    if (increaseReferenceIfExists(hardlinkOrCopiedFile, pipeName, isTsFile)) {
      return getResourceMap(pipeName).get(hardlinkOrCopiedFile.getPath()).getFile();
    }

    // Create the pipe file and the resource that tracks its reference count.
    final File source = Objects.isNull(sourceFile) ? file : sourceFile;
    final File resultFile;

    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final PipeTsFileResource existingResource =
          getResourceMap(pipeName).get(hardlinkOrCopiedFile.getPath());
      if (existingResource != null) {
        existingResource.increaseReferenceCount();
        resultFile = existingResource.getFile();
      } else {
        resultFile =
            isTsFile
                ? createHardLinkOrCopy(source, hardlinkOrCopiedFile, resource)
                : copyFileAtomically(source, hardlinkOrCopiedFile);

        // Track the new pipe file, and record its directory so that dropping the pipe cleans up.
        if (Objects.nonNull(pipeName)) {
          pipeNameToPipeTsFileDirPathMap
              .computeIfAbsent(pipeName, k -> ConcurrentHashMap.newKeySet())
              .add(hardlinkOrCopiedFile.getParentFile().getPath());
          hardlinkOrCopiedFileToPipeTsFileResourceMap
              .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
              .put(resultFile.getPath(), new PipeTsFileResource(resultFile));
        } else {
          hardlinkOrCopiedFileToTsFilePublicResourceMap.put(
              resultFile.getPath(), new PipeTsFilePublicResource(resultFile));
        }
      }
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }
    try {
      increasePublicReference(resultFile, pipeName, isTsFile);
    } catch (final IOException | RuntimeException e) {
      // The private reference must not outlive a failed public reference increase.
      rollbackFileReference(resultFile, pipeName, e);
      throw e;
    }
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
    try {
      increasePublicReference(file, pipeName, isTsFile);
    } catch (final IOException | RuntimeException e) {
      // A private reference must not outlive a failed public reference, otherwise every failed
      // retry pins the pipe file for good.
      rollbackFileReference(file, pipeName, e);
      throw e;
    }
    return true;
  }

  private void rollbackFileReference(
      final File file, final @Nullable String pipeName, final Exception originalException) {
    try {
      decreaseFileReference(file, pipeName, false);
    } catch (final RuntimeException rollbackException) {
      originalException.addSuppressed(rollbackException);
    }
  }

  private void increasePublicReference(
      final File file, final @Nullable String pipeName, final boolean isTsFile) throws IOException {
    if (Objects.isNull(pipeName)) {
      return;
    }
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    increaseFileReference(new File(getCommonFilePath(file)), isTsFile, null, file, null);
  }

  public static File getHardlinkOrCopiedFileInPipeDir(
      final File file, final @Nullable String pipeName) throws IOException {
    return PipeTsFileDirPathResolver.getHardlinkOrCopiedFileInPipeDir(file, pipeName);
  }

  /**
   * Given a hardlink or copied file, decrease its reference count, if the reference count is 0,
   * delete the file. if the given file is not a hardlink or copied file, do nothing.
   *
   * @param hardlinkOrCopiedFile the copied or hard-linked file
   */
  public void decreaseFileReference(
      final File hardlinkOrCopiedFile, final @Nullable String pipeName) {
    decreaseFileReference(hardlinkOrCopiedFile, pipeName, true);
  }

  private void decreaseFileReference(
      final File hardlinkOrCopiedFile,
      final @Nullable String pipeName,
      final boolean decreasePublicReference) {
    segmentLock.lock(hardlinkOrCopiedFile);
    try {
      final String filePath = hardlinkOrCopiedFile.getPath();
      final PipeTsFileResource resource = getResourceMap(pipeName).get(filePath);
      if (resource != null
          && resource.decreaseReferenceCount(shouldDeleteFileWhenNoReference(pipeName))) {
        getResourceMap(pipeName).remove(filePath);
      }
    } finally {
      segmentLock.unlock(hardlinkOrCopiedFile);
    }

    // Decrease the assigner's file to clear hard-link and memory cache
    // Note that it does not exist for historical files
    if (decreasePublicReference) {
      decreasePublicReferenceIfExists(hardlinkOrCopiedFile, pipeName);
    }
  }

  private void decreasePublicReferenceIfExists(final File file, final @Nullable String pipeName) {
    if (Objects.isNull(pipeName)) {
      return;
    }
    // Increase the assigner's file to avoid hard-link or memory cache cleaning
    // Note that it does not exist for historical files
    decreaseFileReference(new File(getCommonFilePath(file)), null);
  }

  private boolean shouldDeleteFileWhenNoReference(
      final @Nullable String pipeTsFileResourcePipeName) {
    return Objects.isNull(pipeTsFileResourcePipeName)
        || !pipeTsFileResourcePipeNameSetUnderDeletion.contains(pipeTsFileResourcePipeName);
  }

  public void markPipeTsFileDirUnderDeletion(final @Nonnull String pipeTsFileResourcePipeName) {
    pipeTsFileResourcePipeNameSetUnderDeletion.add(pipeTsFileResourcePipeName);
  }

  public void unmarkPipeTsFileDirUnderDeletion(final @Nonnull String pipeTsFileResourcePipeName) {
    pipeTsFileResourcePipeNameSetUnderDeletion.remove(pipeTsFileResourcePipeName);
  }

  public void cleanPipeTsFileDir(final @Nonnull String pipeTsFileResourcePipeName) {
    final Set<String> pipeTsFileDirPaths =
        pipeNameToPipeTsFileDirPathMap.remove(pipeTsFileResourcePipeName);
    hardlinkOrCopiedFileToPipeTsFileResourceMap.remove(pipeTsFileResourcePipeName);

    if (Objects.isNull(pipeTsFileDirPaths)) {
      pipeTsFileResourcePipeNameSetUnderDeletion.remove(pipeTsFileResourcePipeName);
      return;
    }

    // The files of one pipe may be scattered on multiple disks, clean every directory of the pipe.
    for (final String pipeTsFileDirPath : pipeTsFileDirPaths) {
      PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner.submitStalePipeDirForPeriodicalCleanup(
          new File(pipeTsFileDirPath));
    }
    pipeTsFileResourcePipeNameSetUnderDeletion.remove(pipeTsFileResourcePipeName);
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

  /** Returns the shared public resource map when {@code pipeName} is null. */
  public Map<String, ? extends PipeTsFileResource> getResourceMap(final @Nullable String pipeName) {
    return Objects.nonNull(pipeName)
        ? hardlinkOrCopiedFileToPipeTsFileResourceMap.computeIfAbsent(
            pipeName, k -> new ConcurrentHashMap<>())
        : hardlinkOrCopiedFileToTsFilePublicResourceMap;
  }

  /** Returns whether a companion mod-file reference was acquired. */
  public boolean pinTsFileResource(
      final TsFileResource resource, final boolean withMods, final @Nullable String pipeName)
      throws IOException {
    final File pinnedTsFile =
        increaseFileReference(resource.getTsFile(), true, pipeName, null, resource);
    try {
      if (withMods && resource.getExclusiveModFile().exists()) {
        increaseFileReference(resource.getExclusiveModFile().getFile(), false, pipeName);
        return true;
      }
      return false;
    } catch (final IOException | RuntimeException e) {
      // Pinning is a two-file operation.  Do not leave the TsFile pinned when the mod file cannot
      // be pinned (for example, when the pipe directory is temporarily unavailable).
      try {
        decreaseFileReference(pinnedTsFile, pipeName);
      } catch (final RuntimeException rollbackException) {
        e.addSuppressed(rollbackException);
      }
      throw e;
    }
  }

  public void unpinTsFileResource(
      final TsFileResource resource, final @Nullable String pipeName, final boolean pinnedModFile)
      throws IOException {
    Exception firstException = null;
    try {
      decreaseFileReference(
          getHardlinkOrCopiedFileInPipeDir(resource.getTsFile(), pipeName), pipeName);
    } catch (final IOException | RuntimeException e) {
      firstException = e;
    }

    // Clean up the mod file even if the TsFile failed, and only if it was pinned: compaction can
    // remove the original mod file.
    if (pinnedModFile) {
      try {
        decreaseFileReference(
            getHardlinkOrCopiedFileInPipeDir(resource.getExclusiveModFile().getFile(), pipeName),
            pipeName);
      } catch (final IOException | RuntimeException e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }

    if (firstException != null) {
      if (firstException instanceof IOException) {
        throw (IOException) firstException;
      }
      throw (RuntimeException) firstException;
    }
  }

  /**
   * Hardlinks the source file into the pipe directory, or copies it if the hardlink can not be
   * created, e.g. across disks. A TsFile which is not closed yet is never copied, because the copy
   * would be truncated: the pin fails instead, and the TsFile is extracted after it is closed.
   *
   * @param resource the source resource of the file, or {@code null} if the file is already final
   */
  private static File createHardLinkOrCopy(
      final File sourceFile,
      final File hardlinkOrCopiedFile,
      final @Nullable TsFileResource resource)
      throws IOException {
    if (sourceFile.getAbsoluteFile().equals(hardlinkOrCopiedFile.getAbsoluteFile())) {
      // A pipe file left by a previous process: never link it onto itself, and never delete the
      // target of a failed link, because that target is the source here.
      if (!hardlinkOrCopiedFile.isFile()) {
        throw new IOException(
            String.format(
                DataNodePipeMessages
                    .EXCEPTION_FAILED_TO_PIN_THE_PIPE_FILE_ARG_BECAUSE_IT_DOES_NOT_EXIST_1741B8E4,
                hardlinkOrCopiedFile.getPath()));
      }
      return hardlinkOrCopiedFile;
    }

    try {
      return FileUtils.createHardLink(sourceFile, hardlinkOrCopiedFile);
    } catch (final IOException | UnsupportedOperationException linkFailure) {
      if (Objects.nonNull(resource) && !resource.isClosed()) {
        final IOException unclosedException =
            new IOException(
                String.format(
                    DataNodePipeMessages
                        .EXCEPTION_FAILED_TO_PIN_THE_TSFILE_ARG_BY_CREATING_A_HARDLINK_AND_THE_TSFILE_CAN_NOT_BE_COPIED_BECAUSE_IT_IS_NOT_CLOSED_YET_4E346D68,
                    sourceFile.getPath()));
        unclosedException.addSuppressed(linkFailure);
        throw unclosedException;
      }

      LOGGER.warn(
          DataNodePipeMessages
              .LOG_FAILED_TO_CREATE_THE_HARDLINK_OF_THE_FILE_ARG_TO_ARG_FALLING_BACK_TO_COPYING_THE_FILE_89FED96B,
          sourceFile.getAbsolutePath(),
          hardlinkOrCopiedFile.getAbsolutePath(),
          linkFailure);
      try {
        return copyFileAtomically(sourceFile, hardlinkOrCopiedFile);
      } catch (final IOException | UnsupportedOperationException copyFailure) {
        copyFailure.addSuppressed(linkFailure);
        throw copyFailure;
      }
    }
  }

  /**
   * Copies the file to a temporary name first, so that a crash or a full disk can never leave a
   * truncated file under the final pipe file name. A copy replaces the target, so that a partial
   * copy left by a previous failure does not block every later pin.
   */
  @TestOnly
  public static File copyFileAtomically(final File sourceFile, final File targetFile)
      throws IOException {
    final File unfinishedFile = new File(targetFile.getPath() + UNFINISHED_FILE_SUFFIX);
    try {
      // A temporary file left by a killed process would fail the copy the same way.
      FileUtils.deleteFileIfExist(unfinishedFile);
      FileUtils.copyFile(sourceFile, unfinishedFile);
      try {
        Files.move(
            unfinishedFile.toPath(),
            targetFile.toPath(),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(
            unfinishedFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      return targetFile;
    } catch (final IOException | RuntimeException e) {
      FileUtils.deleteFileIfExist(unfinishedFile);
      throw e;
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
                LOGGER.warn(DataNodePipeMessages.FAILED_TO_GET_FILE_SIZE_OF_LINKED, resource, e);
                return 0;
              }
            })
        .sum();
  }
}
