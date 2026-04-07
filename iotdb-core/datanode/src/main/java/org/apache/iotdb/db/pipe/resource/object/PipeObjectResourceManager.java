/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.resource.object;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceSegmentLock;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle, physical directories, and reference counting of object resources
 * associated with {@link TsFileResource}s in the IoTDB Pipe module.
 *
 * <p>This manager provides thread-safe operations using segment locks based on the underlying
 * {@link File} instances. It maintains a strictly isolated physical directory structure for
 * different pipes and their respective TsFiles, completely preventing data cross-contamination
 * during pipe transmission.
 *
 * <p><b>Concurrency Note:</b> The internal core data structures utilize {@link ConcurrentHashMap},
 * while fine-grained synchronization is guaranteed via {@link PipeTsFileResourceSegmentLock}. This
 * design ensures high throughput while effectively preventing race conditions that could occur
 * during concurrent file hardlinking or resource cleanup.
 */
public class PipeObjectResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeObjectResourceManager.class);

  // Constants used for directory structure resolution
  private static final String OBJECT_SUBDIR_NAME = "object";
  private static final String SEQUENCE_DIR_NAME = "sequence";
  private static final String UNSEQUENCE_DIR_NAME = "unsequence";

  private static final String ERR_PIPE_NAME_NULL = "Pipe name cannot be null.";
  private static final String ERR_TSFILE_RESOURCE_INVALID =
      "TsFileResource or its underlying TsFile is null.";
  private static final String ERR_DATA_DIR_NOT_FOUND =
      "Could not locate the sequence/unsequence data directory context for TsFile: %s";
  private static final String ERR_DIR_CREATION_FAILED =
      "Failed to create object resource directory: %s";
  private static final String ERR_RESOURCE_MISSING =
      "Object resource missing for TsFile %s in pipe %s. Ensure linkObjectFiles is called prior "
          + "to referencing.";

  /**
   * Two-level concurrent mapping structure: PipeName -> TsFileKey -> PipeObjectResource. Used to
   * globally track all active object resources currently managed by the pipe engine.
   */
  private final Map<String, Map<String, PipeObjectResource>> pipeToTsFileResourceMap =
      new ConcurrentHashMap<>();

  /**
   * Segment lock to guarantee thread safety for concurrent operations (e.g., link, cleanup) on the
   * same TsFile.
   */
  private final PipeTsFileResourceSegmentLock segmentLock = new PipeTsFileResourceSegmentLock();

  /**
   * Caches the base directory for all Pipe Object files. Uses the volatile keyword to ensure
   * multi-thread visibility, avoiding expensive IO path resolution on every operation.
   */
  private volatile File objectBaseDirCache = null;

  /**
   * Hardlinks external object files to the specified TsFile directory under a given Pipe context.
   *
   * @param tsFileResource The target TsFile resource (the subject to be associated).
   * @param pathIterator An iterator providing the relative paths of the object files to be linked.
   * @param pipeName The name of the pipe requesting the linkage.
   * @return The total number of successfully linked object files.
   * @throws IOException If underlying IO exceptions occur during physical directory creation or
   *     hardlinking.
   * @throws IllegalArgumentException If pipeName or tsFileResource is null or invalid.
   */
  public int linkObjectFiles(
      final TsFileResource tsFileResource,
      final Iterator<String> pathIterator,
      final String pipeName)
      throws IOException {

    validatePipeName(pipeName);
    validateTsFileResource(tsFileResource);

    if (pathIterator == null || !pathIterator.hasNext()) {
      return 0;
    }

    final File tsFile = tsFileResource.getTsFile();
    int linkedCount = 0;

    segmentLock.lock(tsFile);
    try {
      final PipeObjectResource resource = getOrCreateObjectResource(tsFileResource, pipeName);

      while (pathIterator.hasNext()) {
        final String relativePath = pathIterator.next();
        if (relativePath != null && !relativePath.trim().isEmpty()) {
          resource.linkObjectFile(relativePath);
          linkedCount++;
        }
      }
      return linkedCount;
    } finally {
      segmentLock.unlock(tsFile);
    }
  }

  /**
   * Increases the reference count of the object resource associated with the given TsFile.
   * <b>Note:</b> A call to this method must be paired with a subsequent call to {@link
   * #decreaseReference} to prevent memory leaks.
   *
   * @param tsFileResource The target TsFile resource.
   * @param pipeName The associated pipe name.
   * @throws IllegalStateException If the resource is not found (i.e., linkObjectFiles was never
   *     called to initialize it).
   */
  public void increaseReference(final TsFileResource tsFileResource, final String pipeName) {
    validatePipeName(pipeName);
    validateTsFileResource(tsFileResource);

    final File tsFile = tsFileResource.getTsFile();
    final String tsFileKey = buildTsFileResourceKeySafely(tsFile);
    segmentLock.lock(tsFile);
    try {
      final PipeObjectResource resource = getPipeResources(pipeName).get(tsFileKey);
      if (resource == null) {
        throw new IllegalStateException(String.format(ERR_RESOURCE_MISSING, tsFileKey, pipeName));
      }
      resource.increaseReferenceCount();
    } finally {
      segmentLock.unlock(tsFile);
    }
  }

  /**
   * Decreases the reference count of the object resource. If the reference count drops to 0 and the
   * file is marked as closed, it will automatically trigger the physical file and memory cleanup of
   * the resource.
   *
   * @param tsFileResource The target TsFile resource.
   * @param pipeName The associated pipe name.
   */
  public void decreaseReference(final TsFileResource tsFileResource, final String pipeName) {
    validatePipeName(pipeName);
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return;
    }

    final File tsFile = tsFileResource.getTsFile();
    final String tsFileKey = buildTsFileResourceKeySafely(tsFile);

    segmentLock.lock(tsFile);
    try {
      final Map<String, PipeObjectResource> pipeResources = getPipeResources(pipeName);
      final PipeObjectResource resource = pipeResources.get(tsFileKey);

      // decreaseReferenceCount() returns true if the reference count reaches zero, meaning it's
      // safe to recycle
      if (resource != null && resource.decreaseReferenceCount()) {
        pipeResources.remove(tsFileKey);
        executeResourceCleanup(resource);
      }
    } finally {
      segmentLock.unlock(tsFile);
    }
  }

  /**
   * Marks the TsFile as "closed" in the specified Pipe context (usually indicating writing is
   * complete or sealed). If there are no linked files mounted at this point, or if the resource's
   * references have been exhausted, the cleanup logic is triggered immediately.
   *
   * @param tsFileResource The target TsFile resource.
   * @param pipeName The associated pipe name.
   */
  public void setTsFileClosed(final TsFileResource tsFileResource, final String pipeName) {
    validatePipeName(pipeName);
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return;
    }

    final File tsFile = tsFileResource.getTsFile();
    final String tsFileKey = buildTsFileResourceKeySafely(tsFile);

    segmentLock.lock(tsFile);
    try {
      final Map<String, PipeObjectResource> pipeResources = getPipeResources(pipeName);
      final PipeObjectResource resource = pipeResources.get(tsFileKey);

      if (resource != null) {
        resource.setTsFileClosed();
        // Cleanup trigger conditions: 1. No linked Object files; OR 2. No external references left
        if (resource.getLinkedFileCount() == 0 || resource.getReferenceCount() <= 0) {
          pipeResources.remove(tsFileKey);
          executeResourceCleanup(resource);
        }
      }
    } finally {
      segmentLock.unlock(tsFile);
    }
  }

  /**
   * Retrieves the physical hardlink file object corresponding to a specific relative path.
   *
   * @param tsFileResource The target TsFile resource.
   * @param relativePath The relative path of the Object within the TsFile's object directory.
   * @param pipeName The associated pipe name.
   * @return The physical {@link File} object of the hardlink; returns {@code null} if not found or
   *     if inputs are invalid.
   */
  public File getObjectFileHardlink(
      final TsFileResource tsFileResource, final String relativePath, final String pipeName) {

    validatePipeName(pipeName);
    if (tsFileResource == null
        || tsFileResource.getTsFile() == null
        || relativePath == null
        || relativePath.trim().isEmpty()) {
      return null;
    }

    final Map<String, PipeObjectResource> pipeResources = pipeToTsFileResourceMap.get(pipeName);
    if (pipeResources == null) {
      return null;
    }

    final PipeObjectResource resource =
        pipeResources.get(buildTsFileResourceKeySafely(tsFileResource.getTsFile()));
    return resource != null ? resource.getObjectFileHardlink(relativePath) : null;
  }

  /**
   * Retrieves the isolated physical directory allocated for a specific TsFile and Pipe, which
   * contains all associated Object files.
   *
   * @param tsFileResource The target TsFile resource.
   * @param pipeName The associated pipe name.
   * @return The directory {@link File} object; returns {@code null} if the resource has not yet
   *     been established.
   */
  public File getLinkedObjectDirectory(final TsFileResource tsFileResource, final String pipeName) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null || pipeName == null) {
      return null;
    }

    final Map<String, PipeObjectResource> pipeResources = pipeToTsFileResourceMap.get(pipeName);
    if (pipeResources == null) {
      return null;
    }

    final PipeObjectResource resource =
        pipeResources.get(buildTsFileResourceKeySafely(tsFileResource.getTsFile()));

    return resource != null ? resource.getObjectFileDir() : null;
  }

  /**
   * Deeply cleans up all object resources associated with the specified Pipe and directly deletes
   * their physical isolated directories. This method is typically called when a user executes a
   * "Drop Pipe" operation.
   *
   * @param pipeName The name of the pipe to be cleaned up.
   */
  public void cleanupPipe(final String pipeName) {
    validatePipeName(pipeName);

    // Remove the pipe's mapping data entirely from the global concurrent map
    final Map<String, PipeObjectResource> removedResources =
        pipeToTsFileResourceMap.remove(pipeName);

    // Clean up internal resource states one by one
    if (removedResources != null) {
      removedResources.values().forEach(this::executeResourceCleanup);
    }

    // Execute cascading deletion of the physical directory
    final File pipeDir = getPipeObjectDir(pipeName);
    if (pipeDir != null && pipeDir.exists()) {
      FileUtils.deleteFileOrDirectory(pipeDir);
    }
  }

  /**
   * Internal core method: Attempts to get the existing ObjectResource. If it does not exist, it
   * instantiates a new resource object and ensures the underlying physical target directory is
   * properly created.
   *
   * @param tsFileResource The target TsFile.
   * @param pipeName The isolated pipe context.
   * @return The initialized or existing {@link PipeObjectResource}.
   * @throws IOException If physical directory creation fails.
   */
  private PipeObjectResource getOrCreateObjectResource(
      final TsFileResource tsFileResource, final String pipeName) throws IOException {
    final File tsFile = tsFileResource.getTsFile();
    final String tsFileKey = buildTsFileResourceKey(tsFile);
    final Map<String, PipeObjectResource> pipeResources = getPipeResources(pipeName);

    PipeObjectResource resource = pipeResources.get(tsFileKey);
    if (resource == null) {
      final File targetObjectDir = getPipeObjectFileDir(tsFile, pipeName);

      // Defensive programming: Handle extremely rare directory creation conflicts in concurrent
      // scenarios
      if (!targetObjectDir.exists() && !targetObjectDir.mkdirs() && !targetObjectDir.exists()) {
        throw new IOException(
            String.format(ERR_DIR_CREATION_FAILED, targetObjectDir.getAbsolutePath()));
      }

      resource = new PipeObjectResource(tsFileResource, targetObjectDir);
      pipeResources.put(tsFileKey, resource);

      // Once successfully created, update the base directory cache
      cacheObjectBaseDir(resource);
    }
    return resource;
  }

  private void executeResourceCleanup(final PipeObjectResource resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.setTsFileClosed();
      resource.cleanup();
    } catch (Exception e) {
      LOGGER.warn(
          "Cleanup failed for object resource associated with TsFile: {}",
          resource.getTsFileResource().getTsFile().getName(),
          e);
    }
  }

  private Map<String, PipeObjectResource> getPipeResources(final String pipeName) {
    return pipeToTsFileResourceMap.computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>());
  }

  private void cacheObjectBaseDir(final PipeObjectResource resource) {
    if (objectBaseDirCache == null) {
      synchronized (this) {
        if (objectBaseDirCache == null) {
          // Based on the known structure: baseDir / pipeName / tsFileKey, backtrack two levels to
          // find the root directory
          objectBaseDirCache = resource.getObjectFileDir().getParentFile().getParentFile();
        }
      }
    }
  }

  private static String buildTsFileResourceKeySafely(final File tsFile) {
    try {
      return buildTsFileResourceKey(tsFile);
    } catch (IOException e) {
      return stripTsFileSuffix(tsFile.getName());
    }
  }

  private static String buildTsFileResourceKey(final File tsFile) throws IOException {
    return buildIsolatedTsFileName(tsFile);
  }

  private static String stripTsFileSuffix(final String tsFileName) {
    return tsFileName != null && tsFileName.endsWith(TsFileConstant.TSFILE_SUFFIX)
        ? tsFileName.substring(0, tsFileName.length() - TsFileConstant.TSFILE_SUFFIX.length())
        : tsFileName;
  }

  /**
   * Quickly concatenates the specific top-level directory for a given Pipe based on the cached base
   * directory.
   */
  private File getPipeObjectDir(final String pipeName) {
    if (objectBaseDirCache == null) {
      return null;
    }
    return new File(objectBaseDirCache, pipeName);
  }

  /**
   * Core addressing logic: Resolves the canonical absolute path used to store object files for a
   * specific TsFile and Pipe.
   *
   * <p><b>Implementation Details:</b> This method traverses up the real path of the TsFile until it
   * finds the 'sequence' or 'unsequence' folder and retrieves its parent directory. This locates
   * the <b>base data directory</b> (usually the underlying data mount point) to which the file
   * belongs. It then derives an exclusive Pipe hardlink output directory structure under this base
   * directory, ensuring physical isolation from the original data write path.
   *
   * @param tsFile The underlying physical file.
   * @param pipeName The pipe context identifier.
   * @return The finally determined target directory for storing Objects.
   * @throws IOException If path resolution fails, or if a standard sequence/unsequence boundary
   *     cannot be found up the file tree.
   */
  public static File getPipeObjectFileDir(final File tsFile, final String pipeName)
      throws IOException {
    validatePipeName(pipeName);
    final File dataContextDir = parseDataContextDir(tsFile);

    // Final directory structure assembly:
    // <dataContextDir>/<pipeHardlinkBase>/object/<pipeName>/<sequenceDir>-<db>-<region>-<timePartition>-<tsFileName>
    final File pipeBaseDir =
        new File(dataContextDir, PipeConfig.getInstance().getPipeHardlinkBaseDirName());
    final File objectCategoryDir = new File(pipeBaseDir, OBJECT_SUBDIR_NAME);
    final File specificPipeDir = new File(objectCategoryDir, pipeName);
    final String isolatedTsFileDirName = buildIsolatedTsFileName(tsFile);
    return new File(specificPipeDir, isolatedTsFileDirName).getCanonicalFile();
  }

  private static String buildIsolatedTsFileName(final File tsFile) throws IOException {
    File currentDir = tsFile.getCanonicalFile().getParentFile();
    String sequenceDir = null;
    String databaseName = null;
    String dataRegionName = null;
    String timePartition = null;

    // Continuously backtrack to the parent until reaching the level explicitly belonging to
    // 'sequence' or 'unsequence'
    while (currentDir != null) {
      final String dirName = currentDir.getName();
      if (SEQUENCE_DIR_NAME.equals(dirName) || UNSEQUENCE_DIR_NAME.equals(dirName)) {
        sequenceDir = dirName;
        break;
      }
      if (timePartition == null) {
        timePartition = dirName;
      } else if (dataRegionName == null) {
        dataRegionName = dirName;
      } else if (databaseName == null) {
        databaseName = dirName;
      }
      currentDir = currentDir.getParentFile();
    }

    return sequenceDir
        + "-"
        + databaseName
        + "-"
        + dataRegionName
        + "-"
        + timePartition
        + "-"
        + stripTsFileSuffix(tsFile.getName());
  }

  private static File parseDataContextDir(final File tsFile) throws IOException {
    File currentDir = tsFile.getCanonicalFile();
    File dataContextDir = null;

    // Continuously backtrack to the parent until reaching the level explicitly belonging to
    // 'sequence' or 'unsequence'
    while (currentDir != null) {
      final String dirName = currentDir.getName();
      if (SEQUENCE_DIR_NAME.equals(dirName) || UNSEQUENCE_DIR_NAME.equals(dirName)) {
        dataContextDir = currentDir.getParentFile();
        break;
      }
      currentDir = currentDir.getParentFile();
    }

    if (dataContextDir == null) {
      throw new IOException(String.format(ERR_DATA_DIR_NOT_FOUND, tsFile.getAbsolutePath()));
    }
    return dataContextDir;
  }

  private static void validatePipeName(final String pipeName) {
    if (pipeName == null) {
      throw new IllegalArgumentException(ERR_PIPE_NAME_NULL);
    }
  }

  private static void validateTsFileResource(final TsFileResource tsFileResource) {
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      throw new IllegalArgumentException(ERR_TSFILE_RESOURCE_INVALID);
    }
  }
}
