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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a managed physical resource for Object-type data associated with a specific {@link
 * TsFileResource} within the Pipe module.
 *
 * <p>This class encapsulates an isolated physical directory used to store hardlinks of Object-type
 * files. Its core design purpose is to ensure that while Pipe tasks are asynchronously transferring
 * these files, the original underlying data files are not accidentally deleted by the storage
 * engine (e.g., during compaction or cleanup tasks) via the establishment of hardlinks.
 *
 * <p><b>Thread Safety:</b> All internal state markers and reference counters utilize atomic
 * variables ({@link AtomicBoolean}, {@link AtomicInteger}). This provides native thread safety for
 * the class when handling high-concurrency scenarios involving link establishment, lifecycle
 * management, and resource reclamation.
 */
public class PipeObjectResource implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeObjectResource.class);

  /**
   * The isolated physical directory dedicated to storing hardlinks of Object-type files associated
   * with this TsFile.
   */
  private final File objectFileDir;

  /** The underlying TsFileResource that logically owns these Object-type files. */
  private final TsFileResource tsFileResource;

  /** Marker indicating whether this resource object has been completely cleaned up and closed. */
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /** Marker indicating whether the underlying TsFile has been marked as closed (sealed). */
  private final AtomicBoolean isTsFileClosed = new AtomicBoolean(false);

  /**
   * Records the total number of Object-type files successfully hardlinked into this isolated
   * directory.
   */
  private final AtomicInteger linkedFileCount = new AtomicInteger(0);

  /**
   * Records the number of active Pipe tasks or events currently holding and using this resource.
   */
  private final AtomicInteger referenceCount = new AtomicInteger(0);

  /**
   * Constructs a new PipeObjectResource instance and ensures the physical isolated directory is
   * properly initialized.
   *
   * @param tsFileResource The associated TsFile resource object.
   * @param objectFileDir The target isolated directory for storing Object-type file hardlinks.
   * @throws IOException If the path exists but is not a directory, or if directory creation fails
   *     due to permission issues, etc.
   */
  public PipeObjectResource(final TsFileResource tsFileResource, final File objectFileDir)
      throws IOException {
    this.tsFileResource = tsFileResource;
    this.objectFileDir = objectFileDir;

    if (objectFileDir.exists()) {
      if (!objectFileDir.isDirectory()) {
        throw new IOException(
            String.format(
                "Object file path exists but is not a directory: %s",
                objectFileDir.getAbsolutePath()));
      }
    } else if (!objectFileDir.mkdirs()) {
      throw new IOException(
          String.format(
              "Failed to create object file directory: %s", objectFileDir.getAbsolutePath()));
    }
  }

  /**
   * Gets the isolated directory containing the Object-type file hardlinks.
   *
   * @return The directory {@link File} object.
   */
  public File getObjectFileDir() {
    return objectFileDir;
  }

  /**
   * Gets the absolute path string of the isolated directory.
   *
   * @return The absolute path string.
   */
  public String getObjectFileDirAbsolutePath() {
    return objectFileDir.getAbsolutePath();
  }

  /**
   * Gets the associated underlying TsFileResource object.
   *
   * @return The {@link TsFileResource} instance.
   */
  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  /**
   * Checks whether this Object resource has been completely closed and cleaned up.
   *
   * @return {@code true} if the resource has been reclaimed, {@code false} otherwise.
   */
  public boolean isClosed() {
    return isResourceClosed.get();
  }

  /**
   * Checks whether the associated underlying TsFile has been marked as closed (sealed).
   *
   * @return {@code true} if the TsFile is closed, {@code false} otherwise.
   */
  public boolean isTsFileClosed() {
    return isTsFileClosed.get();
  }

  /**
   * Marks the associated underlying TsFile as closed (sealed). This is a prerequisite for allowing
   * this resource to be safely destroyed (cleaning up the physical directory) when the reference
   * count drops to zero.
   */
  public void setTsFileClosed() {
    isTsFileClosed.set(true);
  }

  /**
   * Gets the total number of Object-type files currently hardlinked within this resource's isolated
   * directory.
   *
   * @return The count of linked files.
   */
  public int getLinkedFileCount() {
    return linkedFileCount.get();
  }

  /**
   * Gets the number of active references currently holding this resource.
   *
   * @return The current reference count.
   */
  public int getReferenceCount() {
    return referenceCount.get();
  }

  /**
   * Atomically increments the reference count. Indicates that a new Pipe event or task has started
   * actively depending on and using this resource.
   */
  public void increaseReferenceCount() {
    final int count = referenceCount.incrementAndGet();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Increased reference count for object resource of TSFile {}: {}",
          tsFileResource.getTsFile().getPath(),
          count);
    }
  }

  /**
   * Atomically decrements the reference count.
   *
   * @return {@code true} if the reference count drops to 0 <b>AND</b> the associated TsFile is
   *     closed, indicating that this resource is now eligible for physical cleanup; {@code false}
   *     otherwise.
   */
  public boolean decreaseReferenceCount() {
    final int count = referenceCount.decrementAndGet();

    if (count < 0) {
      LOGGER.warn(
          "Reference count for object resource of TSFile {} dropped below 0: {}",
          tsFileResource.getTsFile().getPath(),
          count);
    } else if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Decreased reference count for object resource of TSFile {}: {}",
          tsFileResource.getTsFile().getPath(),
          count);
    }

    // Recycling is only permitted when no tasks are using it and no new data will be written to the
    // underlying TsFile
    return count == 0 && isTsFileClosed.get();
  }

  /**
   * Resolves the original Object-type data file via the {@link TierManager} and creates a hardlink
   * within this resource's physical isolated directory.
   *
   * @param relativePath The relative path of the Object-type file to be linked.
   * @throws IOException If this resource or the TsFile is already closed, the original file cannot
   *     be found across any storage tiers, or the OS-level hardlink creation process fails.
   */
  public void linkObjectFile(final String relativePath) throws IOException {
    if (relativePath == null || relativePath.isEmpty()) {
      return;
    }

    if (isResourceClosed.get()) {
      throw new IOException(
          "Cannot link object files: Object resource is closed for TSFile "
              + tsFileResource.getTsFile().getPath());
    }
    if (isTsFileClosed.get()) {
      throw new IOException(
          "Cannot link object files for closed TSFile: " + tsFileResource.getTsFile().getPath());
    }

    // Attempt to locate the original physical file across different storage tiers
    final String dataRegionId = tsFileResource.getDataRegionId();
    Path relPath = Paths.get(relativePath);
    Path subPath = relPath.subpath(1, relPath.getNameCount());
    Path newRelativePath = Paths.get(dataRegionId).resolve(subPath);
    final Optional<File> originalOpt =
        TierManager.getInstance().getAbsoluteObjectFilePath(newRelativePath.toString(), false);
    if (!originalOpt.isPresent()) {
      throw new IOException(
          "Object file does not exist in any tier (dataRegionId: "
              + dataRegionId
              + ", relative path: "
              + relativePath
              + ")");
    }

    final File hardlinkTargetFile = new File(objectFileDir, relativePath);
    final File parentDir = hardlinkTargetFile.getParentFile();

    // Ensure the specific subdirectory structure exists under the isolated directory before
    // creating the hardlink
    if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
      throw new IOException(
          "Failed to create parent directory for: " + hardlinkTargetFile.getPath());
    }

    if (!hardlinkTargetFile.exists()) {
      FileUtils.createHardLink(originalOpt.get(), hardlinkTargetFile);
      linkedFileCount.incrementAndGet();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Created hardlink for object file: {} -> {}", originalOpt.get(), hardlinkTargetFile);
      }
    }
  }

  /**
   * Gets the physical hardlink file corresponding to the specified relative path.
   *
   * @param relativePath The relative path of the Object-type data file.
   * @return The corresponding hardlink {@link File} object if it exists; {@code null} if the file
   *     does not exist or the resource is closed.
   */
  public File getObjectFileHardlink(final String relativePath) {
    if (relativePath == null || relativePath.isEmpty()) {
      return null;
    }

    if (isResourceClosed.get()) {
      LOGGER.warn(
          "Cannot get object file hardlink: Object resource is closed for TSFile {}",
          tsFileResource.getTsFile().getPath());
      return null;
    }

    final File hardlinkFile = new File(objectFileDir, relativePath);
    return hardlinkFile.exists() ? hardlinkFile : null;
  }

  /**
   * Deep cleanup operation. Marks the resource as closed, recursively deletes the underlying
   * physical isolated directory and all contained hardlinks, and resets all counters.
   */
  public void cleanup() {
    // Ensure the cleanup logic is executed only once in concurrent scenarios
    if (!isResourceClosed.compareAndSet(false, true)) {
      return;
    }

    LOGGER.info(
        "Cleaning up {} object files for TSFile: {}",
        linkedFileCount.get(),
        tsFileResource.getTsFile().getPath());

    if (objectFileDir.exists()) {
      try {
        // Recursively delete the entire physical directory and its internal hardlink files
        FileUtils.deleteFileOrDirectory(objectFileDir, true);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Successfully deleted object file directory: {}", objectFileDir.getAbsolutePath());
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to delete object file directory: {}", objectFileDir.getAbsolutePath(), e);
      }
    }

    linkedFileCount.set(0);
    referenceCount.set(0);
  }

  /**
   * Implements the {@link AutoCloseable} interface. Triggers the cleanup of underlying resources.
   * Logs a warning if closed while active references still exist (which may imply a potential
   * resource leak).
   */
  @Override
  public void close() {
    final int count = referenceCount.get();
    if (count > 0) {
      LOGGER.warn(
          "Closing PipeObjectResource with non-zero active references: {} for TSFile: {}",
          count,
          tsFileResource.getTsFile().getPath());
    }
    cleanup();
  }
}
