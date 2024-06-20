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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq.checkSorted;
import static org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq.sortTablet;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);

  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();
  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong currentBatchId = new AtomicLong(BATCH_ID_GENERATOR.incrementAndGet());
  private final File batchFileBaseDir;

  private static final String TS_FILE_PREFIX = "tb"; // tb means tablet batch
  private final AtomicLong tsFileIdGenerator = new AtomicLong(0);

  private final long maxSizeInBytes;

  private final Map<Pair<String, Long>, Double> pipeName2WeightMap = new HashMap<>();

  private volatile TsFileWriter fileWriter;
  private boolean currentFileNonEmpty = false;
  private final Map<EnrichedEvent, List<Integer>> failedEvent2TabletIndexMap = new HashMap<>();

  PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs);

    this.maxSizeInBytes = requestMaxBatchSizeInBytes;
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

    final File baseDir =
        new File(FOLDER_MANAGER.get().getNextFolder(), Long.toString(currentBatchId.get()));
    if (baseDir.exists()) {
      FileUtils.deleteQuietly(baseDir);
    }
    if (!baseDir.exists() && !baseDir.mkdirs()) {
      LOGGER.warn(
          "Batch id = {}: Failed to create batch file dir {}.",
          currentBatchId.get(),
          baseDir.getPath());
      throw new PipeException(
          String.format(
              "Failed to create batch file dir %s. (Batch id = %s)",
              baseDir.getPath(), currentBatchId.get()));
    }
    LOGGER.info(
        "Batch id = {}: Create batch dir successfully, batch file dir = {}.",
        currentBatchId.get(),
        baseDir.getPath());
    return baseDir;
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event) throws IOException {
    initAndWriteUnSequenceTabletsIfNecessary();
    return writeEventWithSpecifiedIndexes((EnrichedEvent) event, Collections.emptyList());
  }

  private void initAndWriteUnSequenceTabletsIfNecessary() throws IOException {
    if (Objects.isNull(fileWriter)) {
      fileWriter =
          new TsFileWriter(
              new File(
                  batchFileBaseDir,
                  TS_FILE_PREFIX
                      + "_"
                      + IoTDBDescriptor.getInstance().getConfig().getDataNodeId()
                      + "_"
                      + currentBatchId.get()
                      + "_"
                      + tsFileIdGenerator.getAndIncrement()
                      + TsFileConstant.TSFILE_SUFFIX));
    }

    // Only write out-of-order tablets to empty tsFiles to avoid meaningless write in retry.
    // If the current file is non-empty and there exists failed events, we should emit in this round
    // and "onSuccess()" shall be called to clear the previous tsFileWriter.
    if (!currentFileNonEmpty && !failedEvent2TabletIndexMap.isEmpty()) {
      final int size = failedEvent2TabletIndexMap.size();
      LOGGER.info("Rewriting may out-of-order events, event count: {}", size);

      final Map<EnrichedEvent, List<Integer>> originalMap =
          new HashMap<>(failedEvent2TabletIndexMap);
      failedEvent2TabletIndexMap.clear();

      for (final Map.Entry<EnrichedEvent, List<Integer>> entry : originalMap.entrySet()) {
        if (writeEventWithSpecifiedIndexes(entry.getKey(), entry.getValue())) {
          events.add(entry.getKey());
        }
      }

      if (!failedEvent2TabletIndexMap.isEmpty()) {
        LOGGER.info(
            "There are still {} events out of order after retry.",
            failedEvent2TabletIndexMap.size());
      }
    }
  }

  private boolean writeEventWithSpecifiedIndexes(final EnrichedEvent event, List<Integer> indexes)
      throws IOException {
    boolean isSuccessful = true;
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final List<Tablet> tablets = ((PipeInsertNodeTabletInsertionEvent) event).convertToTablets();
      if (indexes.isEmpty()) {
        indexes = IntStream.range(0, tablets.size()).boxed().collect(Collectors.toList());
      }
      for (final Integer index : indexes) {
        final boolean tabletSuccessful =
            writeTablet(
                tablets.get(index),
                ((PipeInsertNodeTabletInsertionEvent) event).isAligned(index),
                event.getPipeName(), event.getCreationTime());
        if (!tabletSuccessful) {
          failedEvent2TabletIndexMap
              .computeIfAbsent(event, failedEvent -> new ArrayList<>())
              .add(index);
        }
        isSuccessful &= tabletSuccessful;
        currentFileNonEmpty |= tabletSuccessful;
      }
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      final boolean tabletSuccessful =
          writeTablet(
              rawTabletInsertionEvent.convertToTablet(),
              rawTabletInsertionEvent.isAligned(),
              rawTabletInsertionEvent.getPipeName(), event.getCreationTime());
      if (!tabletSuccessful) {
        failedEvent2TabletIndexMap.put(rawTabletInsertionEvent, Collections.emptyList());
      }
      isSuccessful &= tabletSuccessful;
      currentFileNonEmpty |= tabletSuccessful;
    } else {
      LOGGER.warn(
          "Batch id = {}: Unsupported event {} type {} when constructing tsfile batch",
          currentBatchId.get(),
          event,
          event.getClass());
    }
    return isSuccessful;
  }

  private boolean writeTablet(final Tablet tablet, final boolean isAligned, final String pipeName, final long creationTime)
      throws IOException {
    if (!checkSorted(tablet)) {
      sortTablet(tablet);
    }
    if (isAligned) {
      try {
        fileWriter.registerAlignedTimeseries(new Path(tablet.deviceId), tablet.getSchemas());
      } catch (final WriteProcessException ignore) {
        // Do nothing if the timeSeries has been registered
      }
      try {
        fileWriter.writeAligned(tablet);
      } catch (final WriteProcessException e) {
        // Handle out-of-order data
        // Do not judge the message for performance
        LOGGER.info(
            "A may out-of-order aligned tablet is encountered, message: {}", e.getMessage());
        return false;
      }
    } else {
      for (final MeasurementSchema schema : tablet.getSchemas()) {
        try {
          fileWriter.registerTimeseries(new Path(tablet.deviceId), schema);
        } catch (final WriteProcessException ignore) {
          // Do nothing if the timeSeries has been registered
        }
      }
      try {
        fileWriter.write(tablet);
      } catch (final WriteProcessException e) {
        // Handle out-of-order data
        // Do not judge the message for performance
        LOGGER.info("A may out-of-order tablet is encountered, message: {}", e.getMessage());
        return false;
      }
    }

    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
    pipeName2WeightMap.compute(new Pair<>(pipeName, creationTime), (name, weight) -> Objects.nonNull(weight) ? ++weight : 1);
    return true;
  }

  @Override
  protected boolean shouldEmit() {
    // If there are out-of-order tablets, we should terminate the tsFile.
    return !failedEvent2TabletIndexMap.isEmpty() && currentFileNonEmpty || super.shouldEmit();
  }

  public Map<Pair<String, Long>, Double> deepCopyPipe2WeightMap() {
    final double sum = pipeName2WeightMap.values().stream().reduce(Double::sum).orElse(0.0);
    if (sum == 0.0) {
      return Collections.emptyMap();
    }
    pipeName2WeightMap.entrySet().forEach(entry -> entry.setValue(entry.getValue() / sum));
    return new HashMap<>(pipeName2WeightMap);
  }

  public synchronized File sealTsFile() throws IOException {
    if (isClosed) {
      return null;
    }

    fileWriter.close();

    final File sealedFile = fileWriter.getIOWriter().getFile();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Batch id = {}: Seal tsfile {} successfully.",
          currentBatchId.get(),
          sealedFile.getPath());
    }
    return fileWriter.getIOWriter().getFile();
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();

    pipeName2WeightMap.clear();

    // We don't need to delete the tsFile here, because the tsFile
    // will be deleted after the file is transferred.
    fileWriter = null;
    currentFileNonEmpty = false;
  }

  @Override
  public synchronized void close() {
    super.close();

    pipeName2WeightMap.clear();
    failedEvent2TabletIndexMap
        .keySet()
        .forEach(event -> event.clearReferenceCount(PipeTabletEventTsFileBatch.class.getName()));
    failedEvent2TabletIndexMap.values().forEach(List::clear);
    failedEvent2TabletIndexMap.clear();

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

  @Override
  synchronized boolean isEmpty() {
    if (isClosed) {
      return true;
    }
    try {
      initAndWriteUnSequenceTabletsIfNecessary();
    } catch (final IOException e) {
      LOGGER.warn("Unexpected IOException in writing unSequence tablets or init tsfile writer", e);
    }
    return !currentFileNonEmpty;
  }
}
