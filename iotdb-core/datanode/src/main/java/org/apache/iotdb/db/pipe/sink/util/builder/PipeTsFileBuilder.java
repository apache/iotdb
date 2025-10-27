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

package org.apache.iotdb.db.pipe.sink.util.builder;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public abstract class PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileBuilder.class);

  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();
  protected final AtomicLong currentBatchId;
  private final File batchFileBaseDir;

  private static final String TS_FILE_PREFIX = "tb"; // tb means tablet batch
  private final AtomicLong tsFileIdGenerator;

  @SuppressWarnings("java:S3077")
  protected volatile TsFileWriter fileWriter;

  public PipeTsFileBuilder(final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    this.currentBatchId = currentBatchId;
    this.tsFileIdGenerator = tsFileIdGenerator;
    try {
      this.batchFileBaseDir = getNextBaseDir();
    } catch (final Exception e) {
      throw new PipeException(
          String.format("Failed to create file dir for batch: %s", e.getMessage()));
    }
  }

  private File getNextBaseDir() throws DiskSpaceInsufficientException {
    if (FOLDER_MANAGER.get() == null) {
      synchronized (FOLDER_MANAGER) {
        if (FOLDER_MANAGER.get() == null) {
          FOLDER_MANAGER.set(
              new FolderManager(
                  Arrays.stream(IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDirs())
                      .map(fileDir -> fileDir + File.separator + ".batch")
                      .collect(Collectors.toList()),
                  DirectoryStrategyType.SEQUENCE_STRATEGY));
        }
      }
    }
    synchronized (FOLDER_MANAGER) {
      File baseDir =
          FOLDER_MANAGER
              .get()
              .getNextWithRetry(
                  folder -> {
                    File dir = new File(folder, Long.toString(currentBatchId.get()));
                    FileUtils.deleteQuietly(dir);
                    if (dir.mkdirs()) {
                      LOGGER.info(
                          "Batch id = {}: Create batch dir successfully, batch file dir = {}.",
                          currentBatchId.get(),
                          dir.getPath());
                      return dir;
                    }
                    LOGGER.warn(
                        "Batch id = {}: Failed to create batch file dir {}.",
                        currentBatchId.get(),
                        dir.getPath());
                    return null;
                  });
      if (baseDir != null) {
        return baseDir;
      }
      throw new PipeException(
          String.format(
              "Failed to create batch file dir %s. (Batch id = %s)",
              baseDir.getPath(), currentBatchId.get()));
    }
  }

  public abstract void bufferTableModelTablet(String dataBase, Tablet tablet);

  public abstract void bufferTreeModelTablet(Tablet tablet, Boolean isAligned);

  public abstract List<Pair<String, File>> convertTabletToTsFileWithDBInfo()
      throws IOException, WriteProcessException;

  public abstract boolean isEmpty();

  public synchronized void onSuccess() {
    fileWriter = null;
  }

  public synchronized void close() {
    if (Objects.nonNull(fileWriter)) {
      try {
        fileWriter.close();
      } catch (final Exception e) {
        LOGGER.info(
            "Batch id = {}: Failed to close the tsfile {} when trying to close batch, because {}",
            currentBatchId.get(),
            fileWriter.getIOWriter().getFile().getPath(),
            e.getMessage(),
            e);
      }

      try {
        FileUtils.delete(fileWriter.getIOWriter().getFile());
      } catch (final Exception e) {
        LOGGER.info(
            "Batch id = {}: Failed to delete the tsfile {} when trying to close batch, because {}",
            currentBatchId.get(),
            fileWriter.getIOWriter().getFile().getPath(),
            e.getMessage(),
            e);
      }

      fileWriter = null;
    }
  }

  protected File createFile() throws IOException {
    return new File(
        batchFileBaseDir,
        TS_FILE_PREFIX
            + "_"
            + IoTDBDescriptor.getInstance().getConfig().getDataNodeId()
            + "_"
            + currentBatchId.get()
            + "_"
            + tsFileIdGenerator.getAndIncrement()
            + TsFileConstant.TSFILE_SUFFIX);
  }
}
