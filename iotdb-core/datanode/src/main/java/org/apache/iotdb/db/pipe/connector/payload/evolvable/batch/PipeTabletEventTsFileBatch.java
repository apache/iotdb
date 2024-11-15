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

import org.apache.iotdb.db.pipe.connector.util.PipeTableModeTsFileBuilder;
import org.apache.iotdb.db.pipe.connector.util.PipeTableModelTabletEventSorter;
import org.apache.iotdb.db.pipe.connector.util.PipeTreeModelTSFileBuilder;
import org.apache.iotdb.db.pipe.connector.util.PipeTreeModelTabletEventSorter;
import org.apache.iotdb.db.pipe.connector.util.PipeTsFileBuilder;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.util.concurrent.atomic.AtomicReference;

public class PipeTabletEventTsFileBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventTsFileBatch.class);

  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();
  private static final AtomicLong BATCH_ID_GENERATOR = new AtomicLong(0);
  private final AtomicLong currentBatchId = new AtomicLong(BATCH_ID_GENERATOR.incrementAndGet());

  private final long maxSizeInBytes;
  private final PipeTsFileBuilder treeModeTsFileBuilder;
  private final PipeTsFileBuilder tableModeTsFileBuilder;

  private final Map<Pair<String, Long>, Double> pipeName2WeightMap = new HashMap<>();

  public PipeTabletEventTsFileBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs);

    this.maxSizeInBytes = requestMaxBatchSizeInBytes;
    AtomicLong tsFileIdGenerator = new AtomicLong(0);
    treeModeTsFileBuilder = new PipeTreeModelTSFileBuilder(currentBatchId, tsFileIdGenerator);
    tableModeTsFileBuilder = new PipeTableModeTsFileBuilder(currentBatchId, tsFileIdGenerator);
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
        if (tablet.rowSize == 0) {
          continue;
        }
        // table Model
        if (isTableModel) {
          bufferTableModelTablet(
              insertNodeTabletInsertionEvent.getPipeName(),
              insertNodeTabletInsertionEvent.getCreationTime(),
              tablet,
              insertNodeTabletInsertionEvent.getTableModelDatabaseName());
          continue;
        }

        // tree Model
        bufferTreeModelTablet(
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
      // table Model
      if (rawTabletInsertionEvent.isTableModelEvent()) {
        bufferTableModelTablet(
            rawTabletInsertionEvent.getPipeName(),
            rawTabletInsertionEvent.getCreationTime(),
            tablet,
            rawTabletInsertionEvent.getTableModelDatabaseName());
        return true;
      }
      // tree Model
      bufferTreeModelTablet(
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

  private void bufferTreeModelTablet(
      final String pipeName,
      final long creationTime,
      final Tablet tablet,
      final boolean isAligned) {
    new PipeTreeModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    treeModeTsFileBuilder.bufferTreeModelTablet(tablet, isAligned);
  }

  private void bufferTableModelTablet(
      final String pipeName, final long creationTime, final Tablet tablet, final String dataBase) {
    final Map<IDeviceID, Pair<Long, Long>> deviceID2TimeRange =
        new PipeTableModelTabletEventSorter(tablet).deduplicateAndSortTimestampsIfNecessary();

    if (deviceID2TimeRange == null) {
      return;
    }

    totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);

    pipeName2WeightMap.compute(
        new Pair<>(pipeName, creationTime),
        (pipe, weight) -> Objects.nonNull(weight) ? ++weight : 1);

    tableModeTsFileBuilder.bufferTableModelTablet(dataBase, tablet, deviceID2TimeRange);
  }

  public Map<Pair<String, Long>, Double> deepCopyPipe2WeightMap() {
    final double sum = pipeName2WeightMap.values().stream().reduce(Double::sum).orElse(0.0);
    if (sum == 0.0) {
      return Collections.emptyMap();
    }
    pipeName2WeightMap.entrySet().forEach(entry -> entry.setValue(entry.getValue() / sum));
    return new HashMap<>(pipeName2WeightMap);
  }

  public synchronized List<Pair<String, File>> sealTsFiles()
      throws IOException, WriteProcessException {
    if (isClosed) {
      return Collections.emptyList();
    }

    List<Pair<String, File>> list = new ArrayList<>();
    if (!treeModeTsFileBuilder.isEmpty()) {
      list.addAll(treeModeTsFileBuilder.sealTsFiles());
    }

    if (!tableModeTsFileBuilder.isEmpty()) {
      list.addAll(tableModeTsFileBuilder.sealTsFiles());
    }
    return list;
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return maxSizeInBytes;
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
