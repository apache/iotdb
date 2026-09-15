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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTableModelTsFileBuilderV2;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTreeModelTsFileBuilderV2;
import org.apache.iotdb.db.pipe.sink.util.builder.PipeTsFileBuilder;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTableModelTabletEventSorter;
import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent.isTabletEmpty;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);

  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong currentBatchId = new AtomicLong(BATCH_ID_GENERATOR.incrementAndGet());

  private final PipeTsFileBuilder treeModeTsFileBuilder;
  private final PipeTsFileBuilder tableModeTsFileBuilder;
  private final TabletTransformer tabletTransformer;

  private final Map<Pair<String, Long>, Double> pipeName2WeightMap = new HashMap<>();

  public PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    this(maxDelayInMs, requestMaxBatchSizeInBytes, null, null, null);
  }

  public PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric) {
    this(maxDelayInMs, requestMaxBatchSizeInBytes, recordMetric, null, null);
  }

  public PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final BiFunction<String, Tablet, Tablet> tableModelTabletPruner) {
    this(maxDelayInMs, requestMaxBatchSizeInBytes, null, tableModelTabletPruner, null);
  }

  public PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TabletTransformer tabletTransformer) {
    this(maxDelayInMs, requestMaxBatchSizeInBytes, null, null, tabletTransformer);
  }

  public PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric,
      final BiFunction<String, Tablet, Tablet> tableModelTabletPruner) {
    this(maxDelayInMs, requestMaxBatchSizeInBytes, recordMetric, tableModelTabletPruner, null);
  }

  private PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric,
      final BiFunction<String, Tablet, Tablet> tableModelTabletPruner,
      final TabletTransformer tabletTransformer) {
    super(maxDelayInMs, requestMaxBatchSizeInBytes, recordMetric);

    final AtomicLong tsFileIdGenerator = new AtomicLong(0);
    treeModeTsFileBuilder = new PipeTreeModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    tableModeTsFileBuilder = new PipeTableModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    this.tabletTransformer =
        Objects.nonNull(tabletTransformer)
            ? tabletTransformer
            : (databaseName, tablet, isTableModel, isAligned) ->
                new TabletTransformResult(
                    Objects.nonNull(tableModelTabletPruner) && isTableModel
                        ? tableModelTabletPruner.apply(databaseName, tablet)
                        : tablet,
                    databaseName,
                    isTableModel,
                    isAligned);
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event) {
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      final boolean isTableModel = insertNodeTabletInsertionEvent.isTableModelEvent();
      final List<Tablet> tablets = insertNodeTabletInsertionEvent.convertToTablets();
      final List<TabletTransformResult> retainedTablets = new ArrayList<>(tablets.size());
      for (int i = 0; i < tablets.size(); ++i) {
        final Tablet tablet = tablets.get(i);
        if (isTabletEmpty(tablet)) {
          continue;
        }
        final TabletTransformResult transformedTablet =
            transformTablet(
                isTableModel ? insertNodeTabletInsertionEvent.getTableModelDatabaseName() : null,
                tablet,
                isTableModel,
                !isTableModel && insertNodeTabletInsertionEvent.isAligned(i));
        if (Objects.isNull(transformedTablet) || isTabletEmpty(transformedTablet.getTablet())) {
          continue;
        }
        retainedTablets.add(transformedTablet);
      }

      // Pruning can remove all rows/columns from a tablet.  Account only for data that is
      // actually retained; otherwise a fully (or partially) pruned event permanently inflates the
      // batch's memory block and can starve TsFile conversion buffers.
      if (retainedTablets.isEmpty()) {
        return false;
      }
      increaseTotalBufferSizeAndUpdateMemoryBlock(
          calculateTabletsSizeInBytes(
              retainedTablets.stream()
                  .map(TabletTransformResult::getTablet)
                  .collect(java.util.stream.Collectors.toList())));
      for (final TabletTransformResult transformedTablet : retainedTablets) {
        if (transformedTablet.isTableModel()) {
          bufferTableModelTablet(
              insertNodeTabletInsertionEvent.getPipeName(),
              insertNodeTabletInsertionEvent.getCreationTime(),
              transformedTablet.getTablet(),
              transformedTablet.getDatabaseName());
        } else {
          bufferTreeModelTablet(
              insertNodeTabletInsertionEvent.getPipeName(),
              insertNodeTabletInsertionEvent.getCreationTime(),
              transformedTablet.getTablet(),
              transformedTablet.isAligned());
        }
      }
      return true;
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      final Tablet tablet = rawTabletInsertionEvent.convertToTablet();
      if (isTabletEmpty(tablet)) {
        return false;
      }
      final boolean isTableModel = rawTabletInsertionEvent.isTableModelEvent();
      final TabletTransformResult transformedTablet =
          transformTablet(
              isTableModel ? rawTabletInsertionEvent.getTableModelDatabaseName() : null,
              tablet,
              isTableModel,
              !isTableModel && rawTabletInsertionEvent.isAligned());
      if (Objects.isNull(transformedTablet) || isTabletEmpty(transformedTablet.getTablet())) {
        return false;
      }
      increaseTotalBufferSizeAndUpdateMemoryBlock(
          calculateTabletSizeInBytes(transformedTablet.getTablet()));
      if (transformedTablet.isTableModel()) {
        bufferTableModelTablet(
            rawTabletInsertionEvent.getPipeName(),
            rawTabletInsertionEvent.getCreationTime(),
            transformedTablet.getTablet(),
            transformedTablet.getDatabaseName());
      } else {
        bufferTreeModelTablet(
            rawTabletInsertionEvent.getPipeName(),
            rawTabletInsertionEvent.getCreationTime(),
            transformedTablet.getTablet(),
            transformedTablet.isAligned());
      }
      return true;
    } else {
      LOGGER.warn(
          DataNodePipeMessages.BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING,
          currentBatchId.get(),
          event,
          event.getClass());
    }
    return false;
  }

  private TabletTransformResult transformTablet(
      final String databaseName,
      final Tablet tablet,
      final boolean isTableModel,
      final boolean isAligned) {
    return tabletTransformer.transform(databaseName, tablet, isTableModel, isAligned);
  }

  private long calculateTabletsSizeInBytes(final List<Tablet> tablets) {
    return tablets.stream()
        .filter(tablet -> !isTabletEmpty(tablet))
        .mapToLong(PipeTabletEventTsFileBatch::calculateTabletSizeInBytes)
        .sum();
  }

  private static long calculateTabletSizeInBytes(final Tablet tablet) {
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) * 2;
  }

  @Override
  public Object captureBatchState() {
    return new BatchState(
        treeModeTsFileBuilder.createCheckpoint(),
        tableModeTsFileBuilder.createCheckpoint(),
        new HashMap<>(pipeName2WeightMap));
  }

  @Override
  public void rollbackBatchState(final Object state) {
    if (!(state instanceof BatchState)) {
      return;
    }
    final BatchState batchState = (BatchState) state;
    treeModeTsFileBuilder.rollbackToCheckpoint(batchState.treeModeCheckpoint);
    tableModeTsFileBuilder.rollbackToCheckpoint(batchState.tableModeCheckpoint);
    pipeName2WeightMap.clear();
    pipeName2WeightMap.putAll(batchState.pipeName2WeightMap);
  }

  private static final class BatchState {
    private final Object treeModeCheckpoint;
    private final Object tableModeCheckpoint;
    private final Map<Pair<String, Long>, Double> pipeName2WeightMap;

    private BatchState(
        final Object treeModeCheckpoint,
        final Object tableModeCheckpoint,
        final Map<Pair<String, Long>, Double> pipeName2WeightMap) {
      this.treeModeCheckpoint = treeModeCheckpoint;
      this.tableModeCheckpoint = tableModeCheckpoint;
      this.pipeName2WeightMap = pipeName2WeightMap;
    }
  }

  private void bufferTreeModelTablet(
      final String pipeName,
      final long creationTime,
      final Tablet tablet,
      final boolean isAligned) {
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    treeModeTsFileBuilder.bufferTreeModelTablet(tablet, isAligned);
  }

  private void bufferTableModelTablet(
      final String pipeName, final long creationTime, final Tablet tablet, final String dataBase) {
    new PipeTableModelTabletEventSorter(tablet).sortAndDeduplicateByDevIdTimestamp();

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    tableModeTsFileBuilder.bufferTableModelTablet(dataBase, tablet);
  }

  public Map<Pair<String, Long>, Double> deepCopyPipe2WeightMap() {
    final double sum = pipeName2WeightMap.values().stream().reduce(Double::sum).orElse(0.0);
    if (sum == 0.0) {
      return Collections.emptyMap();
    }
    pipeName2WeightMap.entrySet().forEach(entry -> entry.setValue(entry.getValue() / sum));
    return new HashMap<>(pipeName2WeightMap);
  }

  @FunctionalInterface
  public interface TabletTransformer {

    TabletTransformResult transform(
        String databaseName, Tablet tablet, boolean isTableModel, boolean isAligned);
  }

  public static final class TabletTransformResult {

    private final Tablet tablet;
    private final String databaseName;
    private final boolean tableModel;
    private final boolean aligned;

    public TabletTransformResult(
        final Tablet tablet,
        final String databaseName,
        final boolean tableModel,
        final boolean aligned) {
      this.tablet = tablet;
      this.databaseName = databaseName;
      this.tableModel = tableModel;
      this.aligned = aligned;
    }

    public Tablet getTablet() {
      return tablet;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public boolean isTableModel() {
      return tableModel;
    }

    public boolean isAligned() {
      return aligned;
    }
  }

  /**
   * Converts a Tablet to a TSFile and returns the generated TSFile along with its corresponding
   * database name.
   *
   * @return a list of pairs containing the database name and the generated TSFile
   * @throws IOException if an I/O error occurs during the conversion process
   * @throws WriteProcessException if an error occurs during the write process
   */
  public synchronized List<Pair<String, File>> sealTsFiles()
      throws IOException, WriteProcessException {
    if (isClosed) {
      return Collections.emptyList();
    }

    final List<Pair<String, File>> list = new ArrayList<>();
    boolean sealedSuccessfully = false;
    try {
      if (!treeModeTsFileBuilder.isEmpty()) {
        list.addAll(treeModeTsFileBuilder.convertTabletToTsFileWithDBInfo());
      }
      if (!tableModeTsFileBuilder.isEmpty()) {
        list.addAll(tableModeTsFileBuilder.convertTabletToTsFileWithDBInfo());
      }
      sealedSuccessfully = true;
      return list;
    } finally {
      if (!sealedSuccessfully) {
        for (final Pair<String, File> sealedFile : list) {
          if (!org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(sealedFile.right)) {
            LOGGER.warn(DataNodePipeMessages.FAILED_TO_DELETE_BATCH_FILE_THIS_FILE, sealedFile);
          }
        }
      }
    }
  }

  @Override
  protected void clearBatchData() {
    pipeName2WeightMap.clear();
    tableModeTsFileBuilder.onSuccess();
    treeModeTsFileBuilder.onSuccess();
  }

  @Override
  protected void closeBatchData() {
    pipeName2WeightMap.clear();
    try {
      tableModeTsFileBuilder.close();
    } finally {
      treeModeTsFileBuilder.close();
    }
  }
}
