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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.pipe.connector.util.PipeTabletEventSorter;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.session.util.RetryUtils;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

  private final List<Tablet> tabletList = new ArrayList<>();
  private final List<Boolean> isTabletAlignedList = new ArrayList<>();

  @SuppressWarnings("java:S3077")
  private volatile TsFileWriter fileWriter;

  public PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
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
  protected boolean constructBatch(final TabletInsertionEvent event) {
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      final List<Tablet> tablets = insertNodeTabletInsertionEvent.convertToTablets();
      for (int i = 0; i < tablets.size(); ++i) {
        final Tablet tablet = tablets.get(i);
        if (tablet.rowSize == 0) {
          continue;
        }
        bufferTablet(
            insertNodeTabletInsertionEvent.getPipeName(),
            insertNodeTabletInsertionEvent.getCreationTime(),
            tablet,
            insertNodeTabletInsertionEvent.isAligned(i));
      }
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      final Tablet tablet = rawTabletInsertionEvent.convertToTablet();
      if (tablet.rowSize == 0) {
        return true;
      }
      bufferTablet(
          rawTabletInsertionEvent.getPipeName(),
          rawTabletInsertionEvent.getCreationTime(),
          tablet,
          rawTabletInsertionEvent.isAligned());
    } else {
      LOGGER.warn(
          "Batch id = {}: Unsupported event {} type {} when constructing tsfile batch",
          currentBatchId.get(),
          event,
          event.getClass());
    }
    return true;
  }

  private void bufferTablet(
      final String pipeName,
      final long creationTime,
      final Tablet tablet,
      final boolean isAligned) {
    new PipeTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    // TODO: Currently, PipeTsFileBuilderV2 still uses a fallback builder, so memory table writing
    // and storing temporary tablets require double the memory.
    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) * 2;

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    tabletList.add(tablet);
    isTabletAlignedList.add(isAligned);
  }

  public Map<Pair<String, Long>, Double> deepCopyPipe2WeightMap() {
    final double sum = pipeName2WeightMap.values().stream().reduce(Double::sum).orElse(0.0);
    if (sum == 0.0) {
      return Collections.emptyMap();
    }
    pipeName2WeightMap.entrySet().forEach(entry -> entry.setValue(entry.getValue() / sum));
    return new HashMap<>(pipeName2WeightMap);
  }

  public synchronized List<File> sealTsFiles() throws IOException, WriteProcessException {
    if (isClosed) {
      return Collections.emptyList();
    }
    try {
      return new PipeTsFileBuilderV2().writeTabletsToTsFiles();
    } catch (org.apache.iotdb.db.exception.WriteProcessException e) {
      LOGGER.warn(
          "Exception occurred when PipeTsFileBuilderV2 writing tablets to tsfile, use fallback tsfile builder: {}",
          e.getMessage(),
          e);
      return writeTabletsToTsFiles();
    }
  }

  private List<File> writeTabletsToTsFiles() throws IOException, WriteProcessException {
    final Map<String, List<Tablet>> device2Tablets = new HashMap<>();
    final Map<String, Boolean> device2Aligned = new HashMap<>();

    // Sort the tablets by device id
    for (int i = 0, size = tabletList.size(); i < size; ++i) {
      final Tablet tablet = tabletList.get(i);
      final String deviceId = tablet.deviceId;
      device2Tablets.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(tablet);
      device2Aligned.put(deviceId, isTabletAlignedList.get(i));
    }

    // Sort the tablets by start time in each device
    for (final List<Tablet> tablets : device2Tablets.values()) {
      tablets.sort(
          // Each tablet has at least one timestamp
          Comparator.comparingLong(tablet -> tablet.timestamps[0]));
    }

    // Sort the devices by device id
    final List<String> devices = new ArrayList<>(device2Tablets.keySet());
    devices.sort(Comparator.naturalOrder());

    // Replace ArrayList with LinkedList to improve performance
    final LinkedHashMap<String, LinkedList<Tablet>> device2TabletsLinkedList =
        new LinkedHashMap<>();
    for (final String device : devices) {
      device2TabletsLinkedList.put(device, new LinkedList<>(device2Tablets.get(device)));
    }

    // Help GC
    devices.clear();
    device2Tablets.clear();

    // Write the tablets to the tsfile device by device, and the tablets
    // in the same device are written in order of start time. Tablets in
    // the same device should not be written if their time ranges overlap.
    // If overlapped, we try to write the tablets whose device id is not
    // the same as the previous one. For the tablets not written in the
    // previous round, we write them in a new tsfile.
    final List<File> sealedFiles = new ArrayList<>();

    // Try making the tsfile size as large as possible
    while (!device2TabletsLinkedList.isEmpty()) {
      if (Objects.isNull(fileWriter)) {
        fileWriter = new TsFileWriter(createFile());
      }

      try {
        tryBestToWriteTabletsIntoOneFile(device2TabletsLinkedList, device2Aligned);
      } catch (final Exception e) {
        LOGGER.warn(
            "Batch id = {}: Failed to write tablets into tsfile, because {}",
            currentBatchId.get(),
            e.getMessage(),
            e);

        try {
          fileWriter.close();
        } catch (final Exception closeException) {
          LOGGER.warn(
              "Batch id = {}: Failed to close the tsfile {} after failed to write tablets into, because {}",
              currentBatchId.get(),
              fileWriter.getIOWriter().getFile().getPath(),
              closeException.getMessage(),
              closeException);
        } finally {
          // Add current writing file to the list and delete the file
          sealedFiles.add(fileWriter.getIOWriter().getFile());
        }

        for (final File sealedFile : sealedFiles) {
          final boolean deleteSuccess = FileUtils.deleteQuietly(sealedFile);
          LOGGER.warn(
              "Batch id = {}: {} delete the tsfile {} after failed to write tablets into {}. {}",
              currentBatchId.get(),
              deleteSuccess ? "Successfully" : "Failed to",
              sealedFile.getPath(),
              fileWriter.getIOWriter().getFile().getPath(),
              deleteSuccess ? "" : "Maybe the tsfile needs to be deleted manually.");
        }
        sealedFiles.clear();

        fileWriter = null;

        throw e;
      }

      fileWriter.close();
      final File sealedFile = fileWriter.getIOWriter().getFile();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Batch id = {}: Seal tsfile {} successfully.",
            currentBatchId.get(),
            sealedFile.getPath());
      }
      sealedFiles.add(sealedFile);
      fileWriter = null;
    }

    return sealedFiles;
  }

  private Tablet tryBestToAggregateTablets(
      final String deviceId, final LinkedList<Tablet> tablets) {
    if (tablets.isEmpty()) {
      return null;
    }

    // Retrieve the first tablet to serve as the basis for the aggregation
    final Tablet firstTablet = tablets.peekFirst();
    final long[] aggregationTimestamps = firstTablet.timestamps;
    final int aggregationRow = firstTablet.rowSize;
    final int aggregationMaxRow = firstTablet.getMaxRowNumber();

    // Prepare lists to accumulate schemas, values, and bitMaps
    final List<MeasurementSchema> aggregatedSchemas = new ArrayList<>();
    final List<Object> aggregatedValues = new ArrayList<>();
    final List<BitMap> aggregatedBitMaps = new ArrayList<>();

    // Iterate and poll tablets from the head that satisfy the aggregation criteria
    while (!tablets.isEmpty()) {
      final Tablet tablet = tablets.peekFirst();
      if (Arrays.equals(tablet.timestamps, aggregationTimestamps)
          && tablet.rowSize == aggregationRow
          && tablet.getMaxRowNumber() == aggregationMaxRow) {
        // Aggregate the current tablet's data
        aggregatedSchemas.addAll(tablet.getSchemas());
        aggregatedValues.addAll(Arrays.asList(tablet.values));
        aggregatedBitMaps.addAll(Arrays.asList(tablet.bitMaps));
        // Remove the aggregated tablet
        tablets.pollFirst();
      } else {
        // Stop aggregating once a tablet does not meet the criteria
        break;
      }
    }

    // Remove duplicates from aggregatedSchemas, record the index of the first occurrence, and
    // filter out the corresponding values in aggregatedValues and aggregatedBitMaps based on that
    // index
    final Set<MeasurementSchema> seen = new HashSet<>();
    final List<Integer> distinctIndices =
        IntStream.range(0, aggregatedSchemas.size())
            .filter(i -> seen.add(aggregatedSchemas.get(i))) // Only keep the first occurrence index
            .boxed()
            .collect(Collectors.toList());

    // Construct a new aggregated Tablet using the deduplicated data
    return new Tablet(
        deviceId,
        distinctIndices.stream().map(aggregatedSchemas::get).collect(Collectors.toList()),
        aggregationTimestamps,
        distinctIndices.stream().map(aggregatedValues::get).toArray(),
        distinctIndices.stream().map(aggregatedBitMaps::get).toArray(BitMap[]::new),
        aggregationRow);
  }

  private void tryBestToWriteTabletsIntoOneFile(
      final LinkedHashMap<String, LinkedList<Tablet>> device2TabletsLinkedList,
      final Map<String, Boolean> device2Aligned)
      throws IOException, WriteProcessException {
    final Iterator<Map.Entry<String, LinkedList<Tablet>>> iterator =
        device2TabletsLinkedList.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, LinkedList<Tablet>> entry = iterator.next();
      final String deviceId = entry.getKey();
      final LinkedList<Tablet> tablets = entry.getValue();

      final List<Tablet> tabletsToWrite = new ArrayList<>();

      Tablet lastTablet = null;
      while (!tablets.isEmpty()) {
        final Tablet tablet = tryBestToAggregateTablets(deviceId, tablets);
        if (Objects.isNull(lastTablet)
            // lastTablet.rowSize is not 0
            || lastTablet.timestamps[lastTablet.rowSize - 1] < tablet.timestamps[0]) {
          tabletsToWrite.add(tablet);
          lastTablet = tablet;
        } else {
          tablets.addFirst(tablet);
          break;
        }
      }

      if (tablets.isEmpty()) {
        iterator.remove();
      }

      final boolean isAligned = device2Aligned.get(deviceId);
      if (isAligned) {
        final Map<String, List<MeasurementSchema>> deviceId2MeasurementSchemas = new HashMap<>();
        tabletsToWrite.forEach(
            tablet ->
                deviceId2MeasurementSchemas.compute(
                    tablet.deviceId,
                    (k, v) -> {
                      if (Objects.isNull(v)) {
                        return new ArrayList<>(tablet.getSchemas());
                      }
                      v.addAll(tablet.getSchemas());
                      return v;
                    }));
        for (final Entry<String, List<MeasurementSchema>> deviceIdWithMeasurementSchemas :
            deviceId2MeasurementSchemas.entrySet()) {
          fileWriter.registerAlignedTimeseries(
              new Path(deviceIdWithMeasurementSchemas.getKey()),
              deviceIdWithMeasurementSchemas.getValue());
        }
        for (final Tablet tablet : tabletsToWrite) {
          fileWriter.writeAligned(tablet);
        }
      } else {
        for (final Tablet tablet : tabletsToWrite) {
          for (final MeasurementSchema schema : tablet.getSchemas()) {
            try {
              fileWriter.registerTimeseries(new Path(tablet.deviceId), schema);
            } catch (final WriteProcessException ignore) {
              // Do nothing if the timeSeries has been registered
            }
          }

          fileWriter.write(tablet);
        }
      }
    }
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();

    pipeName2WeightMap.clear();

    tabletList.clear();
    isTabletAlignedList.clear();

    // We don't need to delete the tsFile here, because the tsFile
    // will be deleted after the file is transferred.
    fileWriter = null;
  }

  @Override
  public synchronized void close() {
    super.close();

    pipeName2WeightMap.clear();

    tabletList.clear();
    isTabletAlignedList.clear();

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
        RetryUtils.retryOnException(() -> FileUtils.delete(fileWriter.getIOWriter().getFile()));
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

  /////////////////////// PipeTsFileBuilderV2 //////////////////////////

  private static final PlanNodeId PLACEHOLDER_PLAN_NODE_ID =
      new PlanNodeId("PipeTreeModelTsFileBuilderV2");

  private class PipeTsFileBuilderV2 {

    private List<File> writeTabletsToTsFiles()
        throws org.apache.iotdb.db.exception.WriteProcessException {
      final IMemTable memTable = new PrimitiveMemTable(null, null);
      final List<File> sealedFiles = new ArrayList<>();
      try (final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(createFile())) {
        writeTabletsIntoOneFile(memTable, writer);
        sealedFiles.add(writer.getFile());
      } catch (final Exception e) {
        LOGGER.warn(
            "Batch id = {}: Failed to write tablets into tsfile, because {}",
            currentBatchId.get(),
            e.getMessage(),
            e);
        // TODO: handle ex
        throw new org.apache.iotdb.db.exception.WriteProcessException(e);
      } finally {
        memTable.release();
      }

      return sealedFiles;
    }

    private void writeTabletsIntoOneFile(
        final IMemTable memTable, final RestorableTsFileIOWriter writer) throws Exception {
      for (int i = 0, size = tabletList.size(); i < size; ++i) {
        final Tablet tablet = tabletList.get(i);

        // convert date value to int
        // refer to
        // org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunk.writeNonAlignedTablet
        final Object[] values = tablet.values;
        for (int j = 0; j < tablet.getSchemas().size(); ++j) {
          final MeasurementSchema schema = tablet.getSchemas().get(j);
          if (Objects.nonNull(schema) && Objects.equals(TSDataType.DATE, schema.getType())) {
            final LocalDate[] dates = ((LocalDate[]) values[j]);
            final int[] dateValues = new int[dates.length];
            for (int k = 0; k < Math.min(dates.length, tablet.rowSize); k++) {
              dateValues[k] = DateUtils.parseDateExpressionToInt(dates[k]);
            }
            values[j] = dateValues;
          }
        }

        final InsertTabletNode insertTabletNode =
            new InsertTabletNode(
                PLACEHOLDER_PLAN_NODE_ID,
                new PartialPath(tablet.deviceId),
                isTabletAlignedList.get(i),
                tablet.getSchemas().stream()
                    .map(MeasurementSchema::getMeasurementId)
                    .toArray(String[]::new),
                tablet.getSchemas().stream()
                    .map(MeasurementSchema::getType)
                    .toArray(TSDataType[]::new),
                // TODO: cast
                tablet.getSchemas().toArray(new MeasurementSchema[0]),
                tablet.timestamps,
                tablet.bitMaps,
                tablet.values,
                tablet.rowSize);

        final int start = 0;
        final int end = insertTabletNode.getRowCount();

        try {
          if (insertTabletNode.isAligned()) {
            memTable.insertAlignedTablet(insertTabletNode, start, end);
          } else {
            memTable.insertTablet(insertTabletNode, start, end);
          }
        } catch (final org.apache.iotdb.db.exception.WriteProcessException e) {
          throw new org.apache.iotdb.db.exception.WriteProcessException(e);
        }
      }

      final MemTableFlushTask memTableFlushTask =
          new MemTableFlushTask(memTable, writer, null, null);
      memTableFlushTask.syncFlushMemTable();

      writer.endFile();
    }
  }
}
