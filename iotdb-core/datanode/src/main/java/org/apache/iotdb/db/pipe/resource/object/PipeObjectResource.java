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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage Object file resources under TSFile - Know the absolute path of Object files - Track
 * whether TSFile is closed - Count how many Object files are linked - Provide hard link
 * functionality - Provide methods to get events and iterators
 */
public class PipeObjectResource implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeObjectResource.class);

  // Object file directory
  private final File objectFileDir;

  // Flag indicating whether Object resource is closed
  private final AtomicBoolean isObjectResourceClosed = new AtomicBoolean(false);

  // Flag indicating whether TSFile is closed
  private final AtomicBoolean isTsFileClosed = new AtomicBoolean(false);

  // Count of linked Object files
  private final AtomicInteger linkedObjectFileCount = new AtomicInteger(0);

  // Reference count, used to track how many places are using this Object resource
  private final AtomicInteger referenceCount = new AtomicInteger(0);

  // Associated TSFile resource
  private final TsFileResource tsFileResource;

  public PipeObjectResource(final TsFileResource tsFileResource, final File objectFileDir)
      throws IOException {
    this.tsFileResource = tsFileResource;

    // Check if File exists and its type
    if (objectFileDir.exists()) {
      if (objectFileDir.isFile()) {
        throw new IOException(
            String.format(
                "Object file directory cannot be a file: %s", objectFileDir.getAbsolutePath()));
      }
      // If exists and is a directory, use it normally
      this.objectFileDir = objectFileDir;
    } else {
      // If does not exist, create as directory
      if (!objectFileDir.mkdirs()) {
        throw new IOException(
            String.format(
                "Failed to create object file directory: %s", objectFileDir.getAbsolutePath()));
      }
      this.objectFileDir = objectFileDir;
    }
  }

  /**
   * Get Object file directory
   *
   * @return Object file directory
   */
  public File getObjectFileDir() {
    return objectFileDir;
  }

  /**
   * Get absolute path of Object file directory
   *
   * @return absolute path of Object file directory
   */
  public String getObjectFileDirAbsolutePath() {
    return objectFileDir.getAbsolutePath();
  }

  /**
   * Get associated TSFile resource
   *
   * @return TSFile resource
   */
  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  /**
   * Get whether Object resource is closed
   *
   * @return true if closed, false if not closed
   */
  public boolean isObjectResourceClosed() {
    return isObjectResourceClosed.get();
  }

  /**
   * Get flag indicating whether TSFile is closed
   *
   * @return true if TSFile is closed, false if not closed
   */
  public boolean isTsFileClosed() {
    return isTsFileClosed.get();
  }

  /** Set TSFile closed flag */
  public void setTsFileClosed() {
    isTsFileClosed.set(true);
  }

  /**
   * Get count of linked Object files
   *
   * @return Object file count
   */
  public int getLinkedObjectFileCount() {
    return linkedObjectFileCount.get();
  }

  /**
   * Get reference count
   *
   * @return reference count
   */
  public int getReferenceCount() {
    return referenceCount.get();
  }

  /** Increase reference count. Called when a new place needs to use this Object resource */
  public void increaseReferenceCount() {
    final int count = referenceCount.incrementAndGet();
    LOGGER.debug(
        "Increased reference count for object resource of TSFile {}: {}",
        tsFileResource.getTsFile().getPath(),
        count);
  }

  /**
   * Decrease reference count. Called when a place no longer uses this Object resource
   *
   * @return true if reference count is 0, can be cleaned up; false if there are still other places
   *     using it
   */
  public boolean decreaseReferenceCount() {
    final int count = referenceCount.decrementAndGet();
    LOGGER.debug(
        "Decreased reference count for object resource of TSFile {}: {}",
        tsFileResource.getTsFile().getPath(),
        count);

    if (count < 0) {
      LOGGER.warn(
          "Reference count for object resource of TSFile {} is decreased to below 0: {}",
          tsFileResource.getTsFile().getPath(),
          count);
    }

    return count == 0 && isTsFileClosed.get();
  }

  /**
   * Convert each relative path to Object absolute path and create hard links
   *
   * @param relativePaths list of Object file relative paths (relative to TSFile directory)
   * @return true if successfully linked, false if failed
   * @throws IOException when hard link creation fails
   */
  public boolean linkObjectFiles(final List<String> relativePaths) throws IOException {
    if (relativePaths == null || relativePaths.isEmpty()) {
      return true; // Empty list is considered success
    }

    // Check if Object resource is closed
    if (isObjectResourceClosed.get()) {
      LOGGER.warn(
          "Cannot link object files: Object resource is closed for TSFile {}",
          tsFileResource.getTsFile().getPath());
      return false;
    }

    // Check if TSFile is closed
    if (isTsFileClosed.get()) {
      LOGGER.warn(
          "Cannot link object files for closed TSFile: {}", tsFileResource.getTsFile().getPath());
      return false;
    }

    final File tsFile = tsFileResource.getTsFile();
    final File tsFileDir = tsFile.getParentFile();
    boolean allSuccess = true;

    for (final String relativePath : relativePaths) {
      if (relativePath == null || relativePath.isEmpty()) {
        continue;
      }

      try {
        // Build complete path of original Object file (relative to TSFile directory)
        final File originalObjectFile = new File(tsFileDir, relativePath);
        if (!originalObjectFile.exists()) {
          LOGGER.warn(
              "Object file does not exist: {} (relative to TSFile: {})",
              relativePath,
              tsFile.getPath());
          allSuccess = false;
          continue;
        }

        // Build hard link target path (in pipe directory)
        final File hardlinkTargetFile = new File(objectFileDir, relativePath);
        hardlinkTargetFile.getParentFile().mkdirs();

        // Create hard link
        FileUtils.createHardLink(originalObjectFile, hardlinkTargetFile);

        linkedObjectFileCount.incrementAndGet();

        LOGGER.debug(
            "Created hardlink for object file: {} -> {}", originalObjectFile, hardlinkTargetFile);
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to create hardlink for object file {} (relative to TSFile {}): {}",
            relativePath,
            tsFile.getPath(),
            e.getMessage(),
            e);
        allSuccess = false;
      }
    }

    return allSuccess;
  }

  /**
   * Get Object file hard link File object based on relative path
   *
   * @param relativePath Object file relative path (relative to TSFile directory)
   * @return Object file hard link File object, or null if file does not exist
   */
  public File getObjectFileHardlink(final String relativePath) {
    if (relativePath == null || relativePath.isEmpty()) {
      return null;
    }

    // Check if Object resource is closed
    if (isObjectResourceClosed.get()) {
      LOGGER.warn(
          "Cannot get object file hardlink: Object resource is closed for TSFile {}",
          tsFileResource.getTsFile().getPath());
      return null;
    }

    // Get Object file from hard link directory
    final File hardlinkFile = new File(objectFileDir, relativePath);

    if (!hardlinkFile.exists()) {
      LOGGER.debug(
          "Object file hardlink does not exist: {} (relative to TSFile: {})",
          relativePath,
          tsFileResource.getTsFile().getPath());
      return null;
    }

    return hardlinkFile;
  }

  /**
   * Get ObjectNode iterator. Note: This method needs to be implemented according to actual
   * requirements
   *
   * @return ObjectNode iterator
   */
  public Iterator<Object> getObjectNodeIterator() {
    // TODO: Implement according to actual requirements, parse ObjectNode from Object files and
    // return iterator
    return new Iterator<Object>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Object next() {
        return null;
      }
    };
  }

  /**
   * Clean up all Object file hard links. Delete the entire Object file directory, including the
   * directory itself
   */
  public void cleanup() {
    // Mark resource as closed to prevent subsequent operations
    isObjectResourceClosed.set(true);

    LOGGER.info(
        "Cleaning up {} object files for TSFile: {}",
        linkedObjectFileCount.get(),
        tsFileResource.getTsFile().getPath());

    // Delete the entire Object file directory, including the directory itself
    // Directory structure: pipe/object/[pipeName]/[tsfilename]/
    // Need to delete [tsfilename] directory and all files under it
    if (objectFileDir.exists()) {
      try {
        FileUtils.deleteFileOrDirectory(objectFileDir, true);
        LOGGER.info(
            "Successfully deleted object file directory: {}", objectFileDir.getAbsolutePath());
      } catch (final Exception e) {
        LOGGER.error(
            "Failed to delete object file directory: {}", objectFileDir.getAbsolutePath(), e);
      }
    }

    linkedObjectFileCount.set(0);
    referenceCount.set(0);
  }

  /**
   * Implement AutoCloseable interface. Automatically called when resource is no longer used, clean
   * up all Object files
   */
  @Override
  public void close() {
    if (referenceCount.get() > 0) {
      LOGGER.warn(
          "Closing PipeObjectResource with non-zero reference count: {} for TSFile: {}",
          referenceCount.get(),
          tsFileResource.getTsFile().getPath());
    }
    cleanup();
  }
}
