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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);
  private static final String TS_FILE_NAME = "sender_batch.tsfile";
  protected final AtomicReference<File> batchFileDirWithIdSuffix = new AtomicReference<>();

  // Used to generate transfer id, which is used to identify a tsfile batch instance
  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicLong batchId = new AtomicLong(0);
  private static final List<String> BATCH_FILE_BASE_DIRS =
      Arrays.stream(IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDirs())
          .map(fileDir -> fileDir + File.separator + ".batch")
          .collect(Collectors.toList());
  private static FolderManager folderManager = null;

  private final long maxSizeInBytes;
  private TsFileWriter fileWriter;
  private Map<String, Double> pipeName2WeightMap = new HashMap<>();

  static {
    try {
      folderManager =
          new FolderManager(BATCH_FILE_BASE_DIRS, DirectoryStrategyType.SEQUENCE_STRATEGY);
    } catch (final DiskSpaceInsufficientException e) {
      LOGGER.error(
          "Fail to create pipe receiver file folders allocation strategy because all disks of folders are full.",
          e);
    }
  }

  PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs);
    this.maxSizeInBytes = requestMaxBatchSizeInBytes;

    batchId.set(BATCH_ID_GENERATOR.incrementAndGet());

    // Clear the original batch file dir if exists
    if (batchFileDirWithIdSuffix.get() != null) {
      if (batchFileDirWithIdSuffix.get().exists()) {
        try {
          FileUtils.deleteDirectory(batchFileDirWithIdSuffix.get());
          LOGGER.info(
              "Batch id = {}: Original batch file dir {} was deleted.",
              batchId.get(),
              batchFileDirWithIdSuffix.get().getPath());
        } catch (Exception e) {
          LOGGER.warn(
              "Batch id = {}: Failed to delete original batch file dir {}, because {}.",
              batchId.get(),
              batchFileDirWithIdSuffix.get().getPath(),
              e.getMessage(),
              e);
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Batch id = {}: Original batch file dir {} is not existed. No need to delete.",
              batchId.get(),
              batchFileDirWithIdSuffix.get().getPath());
        }
      }
      batchFileDirWithIdSuffix.set(null);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Batch id = {}: Current batch file dir is null. No need to delete.", batchId.get());
      }
    }

    final String batchFileBaseDir;
    final String errorMsg =
        String.format(
            "Batch id = %s: Failed to init pipe batch file folder manager because all disks of folders are full.",
            batchId.get());
    try {
      batchFileBaseDir = Objects.isNull(folderManager) ? null : folderManager.getNextFolder();
      if (Objects.isNull(batchFileBaseDir)) {
        throw new PipeException(errorMsg);
      }
    } catch (final Exception e) {
      throw new PipeException(errorMsg, e);
    }

    // Create a new batch file dir
    final File newBatchDir = new File(batchFileBaseDir, Long.toString(batchId.get()));
    if (!newBatchDir.exists() && !newBatchDir.mkdirs()) {
      LOGGER.warn(
          "Batch id = {}: Failed to create batch file dir {}.",
          batchId.get(),
          newBatchDir.getPath());
      throw new PipeException(
          String.format("Failed to create batch file dir %s.", newBatchDir.getPath()));
    }
    batchFileDirWithIdSuffix.set(newBatchDir);

    LOGGER.info(
        "Batch id = {}: Create batch dir successfully, batch file dir = {}.",
        batchId.get(),
        newBatchDir.getPath());
  }

  @Override
  protected void constructBatch(final TabletInsertionEvent event)
      throws IOException, WriteProcessException {
    if (Objects.isNull(fileWriter)) {
      fileWriter = new TsFileWriter(new File(batchFileDirWithIdSuffix.get(), TS_FILE_NAME));
    }
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final List<Tablet> tablets = ((PipeInsertNodeTabletInsertionEvent) event).convertToTablets();
      for (int i = 0; i < tablets.size(); ++i) {
        final Tablet tablet = tablets.get(i);
        if (tablet.rowSize == 0) {
          continue;
        }
        if (((PipeInsertNodeTabletInsertionEvent) event).isAligned(i)) {
          fileWriter.writeAligned(tablet);
        } else {
          fileWriter.write(tablet);
        }
        pipeName2WeightMap.compute(
            ((PipeInsertNodeTabletInsertionEvent) event).getPipeName(),
            (pipeName, weight) -> Objects.nonNull(weight) ? ++weight : 1);
      }
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      if (((PipeRawTabletInsertionEvent) event).isAligned()) {
        fileWriter.writeAligned(((PipeRawTabletInsertionEvent) event).convertToTablet());
      } else {
        fileWriter.write(((PipeRawTabletInsertionEvent) event).convertToTablet());
      }
      pipeName2WeightMap.compute(
          ((PipeRawTabletInsertionEvent) event).getPipeName(),
          (pipeName, weight) -> Objects.nonNull(weight) ? ++weight : 1);
    }
  }

  public Map<String, Double> deepCopyPipeName2WeightMap() {
    final double sum =
        pipeName2WeightMap.values().stream().reduce(Double::sum).orElse(Double.MIN_VALUE);
    pipeName2WeightMap.entrySet().forEach(entry -> entry.setValue(entry.getValue() / sum));
    return new HashMap<>(pipeName2WeightMap);
  }

  public File getTsFile() throws IOException {
    fileWriter.close();
    return fileWriter.getIOWriter().getFile();
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
    try {
      FileUtils.delete(fileWriter.getIOWriter().getFile());
      pipeName2WeightMap.clear();
    } catch (final IOException e) {
      throw new PipeException(
          String.format("Failed to delete tsFile %s,", fileWriter.getIOWriter().getFile()), e);
    }
    fileWriter = null;
  }

  @Override
  protected long getTotalSize() {
    return fileWriter.getIOWriter().getFile().length();
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
  }
}
