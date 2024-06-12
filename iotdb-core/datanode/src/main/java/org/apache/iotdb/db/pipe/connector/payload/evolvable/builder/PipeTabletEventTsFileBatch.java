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

import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);
  protected final AtomicReference<File> batchFileDirWithIdSuffix = new AtomicReference<>();

  // Used to generate transfer id, which is used to identify a tsfile batch instance
  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  protected final AtomicLong batchId = new AtomicLong(0);

  private final long maxSizeInBytes;

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
      batchFileBaseDir = getbatchFileBaseDir();
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
      throws WALPipeException, IOException {}

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
  }

  @Override
  protected long getTotalSize() {
    return 0;
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
  }
}
