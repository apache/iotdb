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

package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTableModelTsFileBuilderV2;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTreeModelTsFileBuilderV2;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTsFileBuilder;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTsFileIdGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TsFileSinkWriter - Responsible for writing event data to TSFile and handling Object files
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Use PipeTsFileBuilder to cache TabletInsertionEvent and batch write to TSFile
 *   <li>Copy TSFile corresponding to TsFileInsertionEvent
 *   <li>Handle hard links for Object files
 * </ul>
 */
public class TsFileSinkWriter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSinkWriter.class);

  private static final int TABLET_BUFFER_SIZE = 1000; // Number of Tablets to buffer before flushing

  private final String targetDirectory;
  private final String pipeName;
  private final long creationTime;

  // TSFile Builders
  private final AtomicLong currentBatchId = new AtomicLong(PipeTsFileIdGenerator.getNextBatchId());
  private final AtomicLong tsFileIdGenerator = new AtomicLong(0);
  private PipeTsFileBuilder treeModelBuilder;
  private PipeTsFileBuilder tableModelBuilder;

  private int bufferedTabletCount = 0;

  public TsFileSinkWriter(
      final String targetDirectory, final String pipeName, final long creationTime) {
    this.targetDirectory = targetDirectory;
    this.pipeName = pipeName;
    this.creationTime = creationTime;

    // Initialize Builders
    this.treeModelBuilder = new PipeTreeModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    this.tableModelBuilder = new PipeTableModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
  }

  /** Write TabletInsertionEvent */
  public synchronized void writeTablet(final TabletInsertionEvent event) throws Exception {
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      writeInsertNodeTablet((PipeInsertNodeTabletInsertionEvent) event);
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      writeRawTablet((PipeRawTabletInsertionEvent) event);
    } else {
      LOGGER.warn("Unsupported TabletInsertionEvent type: {}", event.getClass().getName());
    }
  }

  /** Write PipeInsertNodeTabletInsertionEvent */
  private void writeInsertNodeTablet(final PipeInsertNodeTabletInsertionEvent event)
      throws Exception {
    // Immediately handle Object file hard links
    handleObjectFilesForTabletEvent(event);

    // Convert to Tablet list
    final List<Tablet> tablets = event.convertToTablets();
    if (tablets == null || tablets.isEmpty()) {
      return;
    }

    // Process each Tablet
    for (int i = 0; i < tablets.size(); i++) {
      final Tablet tablet = tablets.get(i);
      if (tablet == null || tablet.getRowSize() == 0) {
        continue;
      }

      // Select Builder based on event type
      if (event.isTableModelEvent()) {
        // Table model
        tableModelBuilder.bufferTableModelTablet(
            event.getSourceDatabaseNameFromDataRegion(), tablet);
      } else {
        // Tree model
        treeModelBuilder.bufferTreeModelTablet(tablet, event.isAligned(i));
      }

      bufferedTabletCount++;
    }
  }

  /** Write PipeRawTabletInsertionEvent */
  private void writeRawTablet(final PipeRawTabletInsertionEvent event) throws Exception {
    // Immediately handle Object file hard links
    handleObjectFilesForTabletEvent(event);

    // Get Tablet from event
    final Tablet tablet = event.convertToTablet();
    if (tablet == null || tablet.getRowSize() == 0) {
      return;
    }

    // Select Builder based on event type
    if (event.isTableModelEvent()) {
      // Table model
      tableModelBuilder.bufferTableModelTablet(event.getSourceDatabaseNameFromDataRegion(), tablet);
    } else {
      // Tree model
      treeModelBuilder.bufferTreeModelTablet(tablet, event.isAligned());
    }

    bufferedTabletCount++;
  }

  /**
   * Flush buffered Tablets to TSFile
   *
   * @return list of generated TSFiles
   */
  public List<File> flushTablets() throws Exception {
    final List<File> generatedFiles = new ArrayList<>();

    if (bufferedTabletCount == 0) {
      return generatedFiles;
    }

    try {
      // Flush Tree model Tablets
      if (!treeModelBuilder.isEmpty()) {
        final List<Pair<String, File>> sealedFiles =
            treeModelBuilder.convertTabletToTsFileWithDBInfo();

        for (final Pair<String, File> pair : sealedFiles) {
          final File tsFile = pair.getRight();
          // Generate final file name (using global generator to ensure uniqueness)
          final String finalFileName = generateFinalTsFileName(tsFile.getName());
          final File targetFile = new File(targetDirectory, finalFileName);

          // If target file exists, generate new file name with sequence number
          File actualTargetFile = targetFile;
          long sequence = 0;
          while (actualTargetFile.exists()) {
            sequence++;
            final String nameWithoutSuffix =
                finalFileName.replace(TsFileConstant.TSFILE_SUFFIX, "");
            final String newFileName =
                nameWithoutSuffix + "_" + sequence + TsFileConstant.TSFILE_SUFFIX;
            actualTargetFile = new File(targetDirectory, newFileName);
          }

          // Move file to target directory
          Files.move(
              tsFile.toPath(), actualTargetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

          LOGGER.info("Created TSFile: {}", actualTargetFile.getAbsolutePath());
          generatedFiles.add(actualTargetFile);
        }

        treeModelBuilder.onSuccess();
      }

      // Flush Table model Tablets
      if (!tableModelBuilder.isEmpty()) {
        final List<Pair<String, File>> sealedFiles =
            tableModelBuilder.convertTabletToTsFileWithDBInfo();

        for (final Pair<String, File> pair : sealedFiles) {
          final File tsFile = pair.getRight();
          // Generate final file name (using global generator to ensure uniqueness)
          final String finalFileName = generateFinalTsFileName(tsFile.getName());
          final File targetFile = new File(targetDirectory, finalFileName);

          // If target file exists, generate new file name with sequence number
          File actualTargetFile = targetFile;
          long sequence = 0;
          while (actualTargetFile.exists()) {
            sequence++;
            final String nameWithoutSuffix =
                finalFileName.replace(TsFileConstant.TSFILE_SUFFIX, "");
            final String newFileName =
                nameWithoutSuffix + "_" + sequence + TsFileConstant.TSFILE_SUFFIX;
            actualTargetFile = new File(targetDirectory, newFileName);
          }

          // Move file to target directory
          Files.move(
              tsFile.toPath(), actualTargetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

          LOGGER.info("Created TSFile: {}", actualTargetFile.getAbsolutePath());
          generatedFiles.add(actualTargetFile);
        }

        tableModelBuilder.onSuccess();
      }

    } finally {
      // Reset counter
      bufferedTabletCount = 0;

      // Recreate Builders (using new BatchID)
      currentBatchId.set(PipeTsFileIdGenerator.getNextBatchId());
      treeModelBuilder = new PipeTreeModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
      tableModelBuilder = new PipeTableModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    }

    return generatedFiles;
  }

  /**
   * Check if flush is needed, flush if needed and return generated file list
   *
   * @return list of generated files, empty list if flush is not needed
   * @throws Exception if flush fails
   */
  public List<File> flushTabletsIfNeeded() throws Exception {
    if (bufferedTabletCount >= TABLET_BUFFER_SIZE) {
      return flushTablets();
    }
    return Collections.emptyList();
  }

  /**
   * Handle Object file hard links for TabletInsertionEvent Process immediately when data is
   * captured
   */
  private void handleObjectFilesForTabletEvent(final EnrichedEvent event) throws Exception {
    // First check if there is Object data
    if (!event.hasObjectData()) {
      return;
    }

    // Scan Object data
    event.scanForObjectData();

    final String[] objectPaths = event.getObjectPaths();
    if (objectPaths == null || objectPaths.length == 0) {
      return;
    }

    // Get TsFileResource
    final TsFileResource tsFileResource = (TsFileResource) event.getTsFileResource();
    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      LOGGER.warn("TsFileResource is null, cannot process object files");
      return;
    }

    // Create Object directory: ${targetDirectory}/tablet_objects/
    // Use unified directory to store Tablet Object files
    final File objectDir = new File(targetDirectory, "tablet_objects");
    if (!objectDir.exists() && !objectDir.mkdirs()) {
      throw new PipeException("Failed to create object directory: " + objectDir);
    }

    // Get pipeName (from event)
    final String pipeName = event.getPipeName();

    // Create hard links for each Object file
    int linkedCount = 0;
    for (final String relativePath : objectPaths) {
      if (relativePath == null || relativePath.isEmpty()) {
        continue;
      }

      try {
        // Get Object file hard link File object through PipeObjectResourceManager
        final File hardlinkSourceFile =
            PipeDataNodeResourceManager.object()
                .getObjectFileHardlink(tsFileResource, relativePath, pipeName);

        if (hardlinkSourceFile == null || !hardlinkSourceFile.exists()) {
          LOGGER.warn("Hardlink source file does not exist for relative path: {}", relativePath);
          continue;
        }

        // Target Object file path (maintain relative path)
        final File targetObjectFile = new File(objectDir, relativePath);

        // Ensure parent directory exists
        final File targetObjectFileParent = targetObjectFile.getParentFile();
        if (targetObjectFileParent != null && !targetObjectFileParent.exists()) {
          targetObjectFileParent.mkdirs();
        }

        // Create hard link (skip if already exists)
        if (!targetObjectFile.exists()) {
          FileUtils.createHardLink(hardlinkSourceFile, targetObjectFile);
          linkedCount++;
          LOGGER.debug("Linked object file: {} -> {}", hardlinkSourceFile, targetObjectFile);
        }
      } catch (final Exception e) {
        LOGGER.warn("Failed to link object file {}: {}", relativePath, e.getMessage(), e);
      }
    }

    if (linkedCount > 0) {
      LOGGER.info(
          "Linked {} object files for TabletInsertionEvent to directory: {}",
          linkedCount,
          objectDir.getAbsolutePath());
    }
  }

  /**
   * Copy TSFile corresponding to TsFileInsertionEvent
   *
   * @return copied TSFile, or null if failed
   */
  public synchronized File copyTsFile(final TsFileInsertionEvent event) throws Exception {
    if (!(event instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn("Unsupported TsFileInsertionEvent type: {}", event.getClass().getName());
      return null;
    }

    final PipeTsFileInsertionEvent tsFileEvent = (PipeTsFileInsertionEvent) event;
    final File sourceTsFile = tsFileEvent.getTsFile();

    if (sourceTsFile == null || !sourceTsFile.exists()) {
      LOGGER.warn("Source TSFile does not exist: {}", sourceTsFile);
      return null;
    }

    // First flush currently buffered Tablets
    if (bufferedTabletCount > 0) {
      flushTablets();
    }

    // Generate target file name
    final String targetFileName = sourceTsFile.getName();
    final File targetTsFile = new File(targetDirectory, targetFileName);

    // Copy TSFile
    Files.copy(sourceTsFile.toPath(), targetTsFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    LOGGER.info("Copied TSFile from {} to {}", sourceTsFile, targetTsFile);

    // Handle Object files
    handleObjectFilesForTsFile(tsFileEvent, targetTsFile);

    return targetTsFile;
  }

  /** Get target directory */
  public String getTargetDirectory() {
    return targetDirectory;
  }

  /**
   * Generate final TSFile file name
   *
   * <p>If original file name is a temporary file name (tb_*), keep original file name Otherwise use
   * original file name
   *
   * @param originalFileName original file name
   * @return final file name
   */
  private String generateFinalTsFileName(final String originalFileName) {
    // If already in temporary file name format, keep as is
    if (PipeTsFileIdGenerator.isValidTempFileName(originalFileName)) {
      return originalFileName;
    }
    // Otherwise keep original file name
    return originalFileName;
  }

  /** Handle Object file hard links for TSFile */
  private void handleObjectFilesForTsFile(
      final PipeTsFileInsertionEvent event, final File targetTsFile) throws Exception {
    // Scan Object data
    event.scanForObjectData();

    final String[] objectPaths = event.getObjectPaths();
    if (objectPaths == null || objectPaths.length == 0) {
      LOGGER.debug("No object files to link for TSFile: {}", targetTsFile.getName());
      return;
    }

    // Create Object directory: ${targetDirectory}/${tsfilename}/
    final String tsFileName = targetTsFile.getName();
    final String tsFileNameWithoutSuffix = tsFileName.replace(TsFileConstant.TSFILE_SUFFIX, "");
    final File objectDir = new File(targetDirectory, tsFileNameWithoutSuffix);

    if (!objectDir.exists() && !objectDir.mkdirs()) {
      throw new PipeException("Failed to create object directory: " + objectDir);
    }

    // Get source TSFile directory
    final File sourceTsFile = event.getTsFile();
    final File sourceTsFileDir = sourceTsFile.getParentFile();

    // Create hard links for each Object file
    int linkedCount = 0;
    for (final String relativePath : objectPaths) {
      if (relativePath == null || relativePath.isEmpty()) {
        continue;
      }

      try {
        // Source Object file path (relative to source TSFile directory)
        final File sourceObjectFile = new File(sourceTsFileDir, relativePath);
        if (!sourceObjectFile.exists()) {
          LOGGER.warn("Source object file does not exist: {}", sourceObjectFile);
          continue;
        }

        // Target Object file path (maintain relative path)
        final File targetObjectFile = new File(objectDir, relativePath);

        // Ensure parent directory exists
        final File targetObjectFileParent = targetObjectFile.getParentFile();
        if (targetObjectFileParent != null && !targetObjectFileParent.exists()) {
          targetObjectFileParent.mkdirs();
        }

        // Create hard link
        FileUtils.createHardLink(sourceObjectFile, targetObjectFile);
        linkedCount++;

        LOGGER.debug("Linked object file: {} -> {}", sourceObjectFile, targetObjectFile);
      } catch (final Exception e) {
        LOGGER.warn("Failed to link object file {}: {}", relativePath, e.getMessage(), e);
      }
    }

    LOGGER.info(
        "Linked {} object files for TSFile: {} to directory: {}",
        linkedCount,
        tsFileName,
        objectDir.getAbsolutePath());
  }

  @Override
  public synchronized void close() throws Exception {
    try {
      // Flush remaining Tablets
      if (bufferedTabletCount > 0) {
        flushTablets();
      }
    } finally {
      // Close Builders
      if (treeModelBuilder != null) {
        treeModelBuilder.close();
      }
      if (tableModelBuilder != null) {
        tableModelBuilder.close();
      }
    }

    LOGGER.info("TsFileSinkWriter closed");
  }
}
