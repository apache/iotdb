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

import static org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent.isTabletEmpty;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);

  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong currentBatchId = new AtomicLong(BATCH_ID_GENERATOR.incrementAndGet());

  private final PipeTsFileBuilder treeModeTsFileBuilder;
  private final PipeTsFileBuilder tableModeTsFileBuilder;

  private final Map<Pair<String, Long>, Double> pipeName2WeightMap = new HashMap<>();

  public PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs, requestMaxBatchSizeInBytes, null);

    final AtomicLong tsFileIdGenerator = new AtomicLong(0);
    treeModeTsFileBuilder = new PipeTreeModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    tableModeTsFileBuilder = new PipeTableModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
  }

  public PipeTabletEventTsFileBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric) {
    super(maxDelayInMs, requestMaxBatchSizeInBytes, recordMetric);

    final AtomicLong tsFileIdGenerator = new AtomicLong(0);
    treeModeTsFileBuilder = new PipeTreeModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
    tableModeTsFileBuilder = new PipeTableModelTsFileBuilderV2(currentBatchId, tsFileIdGenerator);
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event) {
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      final boolean isTableModel = insertNodeTabletInsertionEvent.isTableModelEvent();
      final List<Tablet> tablets = insertNodeTabletInsertionEvent.convertToTablets();
      for (int i = 0; i < tablets.size(); ++i) {
        final Tablet tablet = tablets.get(i);
        if (isTabletEmpty(tablet)) {
          continue;
        }
        if (isTableModel) {
          // table Model
          bufferTableModelTablet(
              insertNodeTabletInsertionEvent.getPipeName(),
              insertNodeTabletInsertionEvent.getCreationTime(),
              tablet,
              insertNodeTabletInsertionEvent.getTableModelDatabaseName());
        } else {
          // tree Model
          bufferTreeModelTablet(
              insertNodeTabletInsertionEvent.getPipeName(),
              insertNodeTabletInsertionEvent.getCreationTime(),
              tablet,
              insertNodeTabletInsertionEvent.isAligned(i));
        }
      }
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      final Tablet tablet = rawTabletInsertionEvent.convertToTablet();
      if (isTabletEmpty(tablet)) {
        return true;
      }
      if (rawTabletInsertionEvent.isTableModelEvent()) {
        // table Model
        bufferTableModelTablet(
            rawTabletInsertionEvent.getPipeName(),
            rawTabletInsertionEvent.getCreationTime(),
            tablet,
            rawTabletInsertionEvent.getTableModelDatabaseName());
      } else {
        // tree Model
        bufferTreeModelTablet(
            rawTabletInsertionEvent.getPipeName(),
            rawTabletInsertionEvent.getCreationTime(),
            tablet,
            rawTabletInsertionEvent.isAligned());
      }
    } else {
      LOGGER.warn(
          "Batch id = {}: Unsupported event {} type {} when constructing tsfile batch",
          currentBatchId.get(),
          event,
          event.getClass());
    }
    return true;
  }

  private void bufferTreeModelTablet(
      final String pipeName,
      final long creationTime,
      final Tablet tablet,
      final boolean isAligned) {
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    // TODO: Currently, PipeTreeModelTsFileBuilderV2 still uses PipeTreeModelTsFileBuilder as a
    // fallback builder, so memory table writing and storing temporary tablets require double the
    // memory.
    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) * 2;

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    treeModeTsFileBuilder.bufferTreeModelTablet(tablet, isAligned);
  }

  private void bufferTableModelTablet(
      final String pipeName, final long creationTime, final Tablet tablet, final String dataBase) {
    new PipeTableModelTabletEventSorter(tablet).sortAndDeduplicateByDevIdTimestamp();

    // TODO: Currently, PipeTableModelTsFileBuilderV2 still uses PipeTableModelTsFileBuilder as a
    // fallback builder, so memory table writing and storing temporary tablets require double the
    // memory.
    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) * 2;

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
    if (!treeModeTsFileBuilder.isEmpty()) {
      list.addAll(treeModeTsFileBuilder.convertTabletToTsFileWithDBInfo());
    }
    if (!tableModeTsFileBuilder.isEmpty()) {
      list.addAll(tableModeTsFileBuilder.convertTabletToTsFileWithDBInfo());
    }
    return list;
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();

    pipeName2WeightMap.clear();
    tableModeTsFileBuilder.onSuccess();
    treeModeTsFileBuilder.onSuccess();
  }

  @Override
  public synchronized void close() {
    super.close();

    pipeName2WeightMap.clear();

    tableModeTsFileBuilder.close();
    treeModeTsFileBuilder.close();
  }
}
