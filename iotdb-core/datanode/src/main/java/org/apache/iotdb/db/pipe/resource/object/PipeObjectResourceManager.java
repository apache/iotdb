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

package org.apache.iotdb.db.pipe.resource.object;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceSegmentLock;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage Object file resources under TSFile. Each TSFile corresponds to one PipeObjectResource
 * instance
 */
public class PipeObjectResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeObjectResourceManager.class);

  // PipeName -> (tsFileName -> PipeObjectResource)
  // Used to manage Object files for each TSFile under each Pipe
  private final Map<String, Map<String, PipeObjectResource>> pipeToTsFileToObjectResourceMap =
      new ConcurrentHashMap<>();

  // Cache absolute path of object/ directory: data/pipe/object/
  // Cached on first link, used to clean up entire Pipe directory
  private volatile File objectBaseDirCache = null;

  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

  /////////////////////////////// Core Function Methods ///////////////////////////////

  /**
   * Create hard links for Object files under TSFile
   *
   * @param tsFileResource TSFile resource, used to get TSFile information
   * @param objectFilePaths list of Object file paths (relative paths)
   * @param pipeName pipe name
   * @return true if successfully linked, false if failed
   * @throws IOException when hard link creation fails
   */
  public boolean linkObjectFiles(
      final TsFileResource tsFileResource,
      final List<String> objectFilePaths,
      final @Nullable String pipeName)
      throws IOException {
    final PipeObjectResource objectResource = getOrCreateObjectResource(tsFileResource, pipeName);

    // On first link, cache absolute path of object/ root directory
    if (objectBaseDirCache == null) {
      synchronized (this) {
        if (objectBaseDirCache == null) {
          final File objectFileDir = objectResource.getObjectFileDir();
          // objectFileDir format: data/pipe/object/[pipeName]/[tsfilename]/
          // Go up two levels to get: data/pipe/object/
          File baseDir = objectFileDir.getParentFile(); // data/pipe/object/[pipeName]/
          if (pipeName != null) {
            baseDir = baseDir.getParentFile(); // data/pipe/object/
          }
          objectBaseDirCache = baseDir;
          LOGGER.info("Cached object base directory: {}", objectBaseDirCache.getAbsolutePath());
        }
      }
    }

    // Link Object files
    final boolean success = objectResource.linkObjectFiles(objectFilePaths);

    LOGGER.debug(
        "Linked object files for TSFile: {} (pipe: {}, linked count: {})",
        tsFileResource.getTsFile().getName(),
        pipeName,
        objectResource.getLinkedObjectFileCount());

    return success;
  }

  /**
   * Increase reference count of Object resource
   *
   * @param tsFileResource TSFile resource
   * @param pipeName pipe name
   */
  public void increaseReference(
      final TsFileResource tsFileResource, final @Nullable String pipeName) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      LOGGER.warn("Cannot increase reference: TSFileResource or TSFile is null");
      return;
    }

    final String tsFileName = tsFileResource.getTsFile().getName();
    final PipeObjectResource objectResource = getResourceMap(pipeName).get(tsFileName);

    if (objectResource == null) {
      LOGGER.warn(
          "Cannot increase reference for non-existent object resource: {} (pipe: {})",
          tsFileName,
          pipeName);
      return;
    }

    objectResource.increaseReferenceCount();

    LOGGER.debug(
        "Increased reference for object resource: {} (pipe: {}, ref count: {})",
        tsFileName,
        pipeName,
        objectResource.getReferenceCount());
  }

  /**
   * Decrease reference count of Object resource. When reference count reaches 0, clean up all
   * Object files under this TSFile
   *
   * @param tsFileResource TSFile resource
   * @param pipeName pipe name
   */
  public void decreaseReference(
      final TsFileResource tsFileResource, final @Nullable String pipeName) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return;
    }

    final String tsFileName = tsFileResource.getTsFile().getName();
    final PipeObjectResource objectResource = getResourceMap(pipeName).get(tsFileName);

    if (objectResource == null) {
      LOGGER.warn(
          "Cannot decrease reference for non-existent object resource: {} (pipe: {})",
          tsFileName,
          pipeName);
      return;
    }

    final boolean shouldCleanup = objectResource.decreaseReferenceCount();

    LOGGER.debug(
        "Decreased reference for object resource: {} (pipe: {}, remaining: {})",
        tsFileName,
        pipeName,
        objectResource.getReferenceCount());

    if (shouldCleanup) {
      // Reference count is 0, clean up resource
      cleanupObjectFilesForTsFile(pipeName, tsFileName);
    }
  }

  /**
   * Called when TSFile is closed, set PipeObjectResource's tsFileClosed flag to true Note: Will not
   * immediately clean up Object files, cleanup is managed by reference count
   *
   * @param tsFileResource TSFile resource
   * @param pipeName pipe name
   */
  public void setTsFileClosed(
      final TsFileResource tsFileResource, final @Nullable String pipeName) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return;
    }

    final String tsFileName = tsFileResource.getTsFile().getName();
    final PipeObjectResource objectResource = getResourceMap(pipeName).get(tsFileName);
    if (objectResource != null) {
      objectResource.setTsFileClosed();
      LOGGER.debug(
          "Set tsFileClosed flag for object resource: {} (pipe: {})", tsFileName, pipeName);
    }
  }

  /**
   * Clean up all Object files for specified Pipe. Delete the entire Pipe's Object directory
   *
   * @param pipeName pipe name
   */
  public void cleanupPipe(final @Nullable String pipeName) {
    final String pipeKey = pipeName != null ? pipeName : "null";
    final Map<String, PipeObjectResource> resourceMap =
        pipeToTsFileToObjectResourceMap.remove(pipeKey);

    if (resourceMap == null || resourceMap.isEmpty()) {
      LOGGER.info("No object resources to clean up for pipe: {}", pipeName);
      return;
    }

    LOGGER.info("Cleaning up {} object resources for pipe: {}", resourceMap.size(), pipeName);

    // Clean up Object resources for all TSFiles
    for (final Map.Entry<String, PipeObjectResource> entry : resourceMap.entrySet()) {
      final String tsFileName = entry.getKey();
      final PipeObjectResource objectResource = entry.getValue();
      try {
        objectResource.setTsFileClosed();
        objectResource.cleanup();
        LOGGER.debug("Cleaned up object resource for TSFile: {} (pipe: {})", tsFileName, pipeName);
      } catch (final Exception e) {
        LOGGER.error(
            "Failed to cleanup object resource for TSFile: {} (pipe: {})", tsFileName, pipeName, e);
      }
    }

    // Delete entire Pipe's Object directory: pipe/object/[pipeName]/
    final File pipeObjectDir = getPipeObjectDirForPipe(pipeName);
    if (pipeObjectDir != null && pipeObjectDir.exists()) {
      try {
        FileUtils.deleteFileOrDirectory(pipeObjectDir, true);
        LOGGER.info("Deleted pipe object directory: {}", pipeObjectDir.getAbsolutePath());
      } catch (final Exception e) {
        LOGGER.error("Failed to delete pipe object directory for pipe: {}", pipeName, e);
      }
    } else if (pipeObjectDir == null) {
      LOGGER.warn(
          "Cannot get pipe object directory for pipe: {}, skipping directory deletion", pipeName);
    }
  }

  /////////////////////////////// Helper Methods ///////////////////////////////

  /**
   * Get or create PipeObjectResource corresponding to TSFile
   *
   * @param tsFileResource TSFile resource, used to get TSFile information and file name
   * @param pipeName pipe name, if null means it's assigner's public resource
   * @return PipeObjectResource instance
   * @throws IOException when directory creation fails
   */
  private PipeObjectResource getOrCreateObjectResource(
      final TsFileResource tsFileResource, final @Nullable String pipeName) throws IOException {
    final File tsFile = tsFileResource.getTsFile();
    if (tsFile == null) {
      throw new IOException("TSFile is null in TsFileResource");
    }
    final String tsFileName = tsFile.getName();

    segmentLock.lock(tsFile);
    try {
      return getResourceMap(pipeName)
          .computeIfAbsent(
              tsFileName,
              k -> {
                try {
                  final File objectFileDir = getPipeObjectFileDir(tsFile, pipeName);
                  return new PipeObjectResource(tsFileResource, objectFileDir);
                } catch (final IOException e) {
                  LOGGER.error(
                      "Failed to create PipeObjectResource for TSFile {}: {}",
                      tsFile.getPath(),
                      e.getMessage(),
                      e);
                  throw new RuntimeException(e);
                }
              });
    } finally {
      segmentLock.unlock(tsFile);
    }
  }

  /**
   * Get resource Map for specified Pipe
   *
   * @param pipeName pipe name
   * @return resource Map for this Pipe
   */
  private Map<String, PipeObjectResource> getResourceMap(final @Nullable String pipeName) {
    return pipeToTsFileToObjectResourceMap.computeIfAbsent(
        pipeName != null ? pipeName : "null", k -> new ConcurrentHashMap<>());
  }

  /**
   * Clean up all Object files under specified TSFile
   *
   * @param pipeName pipe name
   * @param tsFileName TSFile file name
   */
  private void cleanupObjectFilesForTsFile(
      final @Nullable String pipeName, final String tsFileName) {
    final Map<String, PipeObjectResource> resourceMap = getResourceMap(pipeName);
    final PipeObjectResource objectResource = resourceMap.remove(tsFileName);

    if (objectResource == null) {
      return;
    }

    LOGGER.info(
        "Cleaning up object files for TSFile: {} (pipe: {}, linked {} files)",
        tsFileName,
        pipeName,
        objectResource.getLinkedObjectFileCount());

    // Mark TSFile as closed
    objectResource.setTsFileClosed();

    // Clean up all Object files
    objectResource.cleanup();
  }

  /////////////////////////////// Query Methods ///////////////////////////////

  /**
   * Get PipeObjectResource for specified TSFile
   *
   * @param tsFileResource TSFile resource
   * @param pipeName pipe name
   * @return PipeObjectResource instance, or null if does not exist
   */
  @Nullable
  public PipeObjectResource getObjectResource(
      final TsFileResource tsFileResource, final @Nullable String pipeName) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return null;
    }
    final String tsFileName = tsFileResource.getTsFile().getName();
    return getResourceMap(pipeName).get(tsFileName);
  }

  /**
   * Get Object file hard link File object based on relative path This is a convenience method that
   * first gets PipeObjectResource, then calls its getObjectFileHardlink method
   *
   * @param tsFileResource TSFile resource
   * @param relativePath Object file relative path (relative to TSFile directory)
   * @param pipeName pipe name
   * @return Object file hard link File object, or null if does not exist
   */
  @Nullable
  public File getObjectFileHardlink(
      final TsFileResource tsFileResource,
      final String relativePath,
      final @Nullable String pipeName) {
    final PipeObjectResource objectResource = getObjectResource(tsFileResource, pipeName);
    if (objectResource == null) {
      return null;
    }
    return objectResource.getObjectFileHardlink(relativePath);
  }

  /**
   * Get Object file count under specified TSFile
   *
   * @param tsFileResource TSFile resource
   * @param pipeName pipe name
   * @return Object file count
   */
  @TestOnly
  public int getObjectFileCount(
      final TsFileResource tsFileResource, final @Nullable String pipeName) {
    final PipeObjectResource objectResource = getObjectResource(tsFileResource, pipeName);
    return objectResource == null ? 0 : objectResource.getLinkedObjectFileCount();
  }

  /**
   * Get Object resource count for specified Pipe
   *
   * @param pipeName pipe name
   * @return Object resource count
   */
  @TestOnly
  public int getPipeObjectResourceCount(final @Nullable String pipeName) {
    final Map<String, PipeObjectResource> resourceMap = getResourceMap(pipeName);
    return resourceMap.size();
  }

  /////////////////////////////// Static Utility Methods ///////////////////////////////

  /**
   * Get Object file hard link path in pipe directory
   *
   * <p>This method mimics the implementation of {@link
   * org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager#getHardlinkOrCopiedFileInPipeDir}
   *
   * @param objectFile original Object file (relative to TSFile directory)
   * @param tsFileResource TSFile resource, used to determine which TSFile the Object file belongs
   *     to
   * @param pipeName pipe name, if null means it's assigner's public resource
   * @return Object file hard link path in pipe directory
   * @throws IOException when path cannot be determined
   */
  public static File getObjectFileHardlinkInPipeDir(
      final File objectFile, final TsFileResource tsFileResource, final @Nullable String pipeName)
      throws IOException {
    try {
      final File tsFile = tsFileResource.getTsFile();
      if (tsFile == null) {
        throw new IOException("TSFile is null in TsFileResource");
      }

      // Get Object file relative path (relative to TSFile directory)
      final String relativePath = getObjectFileRelativePath(objectFile, tsFile);

      // Get Object file directory
      final File objectFileDir = getPipeObjectFileDir(tsFile, pipeName);

      // Return hard link path
      return new File(objectFileDir, relativePath);
    } catch (final Exception e) {
      throw new IOException(
          String.format(
              "failed to get object file hardlink in pipe dir " + "for object file %s, tsfile: %s",
              objectFile.getPath(),
              tsFileResource.getTsFile() != null ? tsFileResource.getTsFile().getPath() : "null"),
          e);
    }
  }

  /**
   * Get Object file relative path relative to TSFile directory
   *
   * @param objectFile Object file
   * @param tsFile TSFile file
   * @return Object file relative path relative to TSFile directory
   * @throws IOException when relative path cannot be calculated
   */
  private static String getObjectFileRelativePath(final File objectFile, final File tsFile)
      throws IOException {
    final File tsFileDir = tsFile.getParentFile();
    if (tsFileDir == null) {
      throw new IOException("TSFile has no parent directory: " + tsFile.getPath());
    }

    final String objectFilePath = objectFile.getCanonicalPath();
    final String tsFileDirPath = tsFileDir.getCanonicalPath();

    if (!objectFilePath.startsWith(tsFileDirPath)) {
      throw new IOException(
          String.format(
              "Object file %s is not under TSFile directory %s", objectFilePath, tsFileDirPath));
    }

    // Calculate relative path
    final String relativePath = objectFilePath.substring(tsFileDirPath.length());
    // Remove leading path separator
    return relativePath.startsWith(File.separator) || relativePath.startsWith("/")
        ? relativePath.substring(1)
        : relativePath;
  }

  /**
   * Get Object file directory in pipe directory
   *
   * <p>Directory structure description:
   *
   * <pre>
   * data/
   *   sequence/
   *     database/
   *       dataRegionId/
   *         timePartitionId/
   *           tsfile.tsfile
   *   pipe/
   *     tsfile/              # TSFile hardlink directory
   *       [pipeName]/        # If pipeName exists, under this subdirectory
   *         tsfile.tsfile    # TSFile hardlink
   *     object/              # Object file hardlink directory (same level as tsfile)
   *       [pipeName]/        # If pipeName exists, under this subdirectory
   *         [tsfilename]/    # Directory named after TSFile, containing all Object files for this TSFile
   *           object1.obj    # Object file hardlink
   *           object2.obj
   * </pre>
   *
   * @param tsFile TSFile file (original file, not hardlink)
   * @param pipeName pipe name, if null means it's assigner's public resource
   * @return Object file directory
   * @throws IOException when path cannot be determined
   */
  private static File getPipeObjectFileDir(final File tsFile, final @Nullable String pipeName)
      throws IOException {
    // Traverse up from TSFile path until finding sequence/unsequence directory
    // Mimic implementation of PipeTsFileResourceManager.getPipeTsFileDirPath
    File file = tsFile;
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FOLDER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
        && !file.getName().equals(PipeConfig.getInstance().getPipeHardlinkBaseDirName())) {
      file = file.getParentFile();
      if (file == null) {
        throw new IOException("Cannot find sequence/unsequence folder for: " + tsFile.getPath());
      }
    }

    // Build object directory path
    // Structure: data/pipe/object/[pipeName]/[tsfilename]/
    // tsfilename is obtained from tsFile.getName()
    File objectDir =
        new File(
            file.getParentFile().getCanonicalPath()
                + File.separator
                + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
                + File.separator
                + "object");

    if (Objects.nonNull(pipeName)) {
      objectDir = new File(objectDir, pipeName);
    }

    // Add TSFile name as subdirectory
    objectDir = new File(objectDir, tsFile.getName());

    return objectDir.getCanonicalFile();
  }

  /**
   * Get Object directory for specified Pipe
   *
   * <p>Use cached object/ root directory to build Pipe's Object directory
   *
   * @param pipeName pipe name
   * @return Pipe's Object directory path: data/pipe/object/[pipeName]/, or null if not cached
   */
  private File getPipeObjectDirForPipe(final @Nullable String pipeName) {
    if (objectBaseDirCache == null) {
      LOGGER.warn(
          "Object base directory not cached yet, cannot get pipe object directory for pipe: {}",
          pipeName);
      return null;
    }

    // Build Pipe's directory from cached root directory
    // objectBaseDirCache: data/pipe/object/
    // Return: data/pipe/object/[pipeName]/
    File pipeObjectDir = objectBaseDirCache;
    if (pipeName != null) {
      pipeObjectDir = new File(pipeObjectDir, pipeName);
    }

    return pipeObjectDir;
  }
}
