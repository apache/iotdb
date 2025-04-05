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

package org.apache.iotdb.db.pipe.connector.util.builder;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTreeModelTsFileBuilder extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTreeModelTsFileBuilder.class);

  private final List<Tablet> tabletList = new ArrayList<>();
  private final List<Boolean> isTabletAlignedList = new ArrayList<>();

  public PipeTreeModelTsFileBuilder(
      final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(final String dataBase, final Tablet tablet) {
    throw new UnsupportedOperationException(
        "PipeTreeModelTsFileBuilder does not support table model tablet to build TSFile");
  }

  @Override
  public void bufferTreeModelTablet(final Tablet tablet, final Boolean isAligned) {
    tabletList.add(tablet);
    isTabletAlignedList.add(isAligned);
  }

  @Override
  public List<Pair<String, File>> convertTabletToTsFileWithDBInfo()
      throws IOException, WriteProcessException {
    return writeTabletsToTsFiles();
  }

  @Override
  public boolean isEmpty() {
    return tabletList.isEmpty();
  }

  @Override
  public void onSuccess() {
    super.onSuccess();
    tabletList.clear();
    isTabletAlignedList.clear();
  }

  @Override
  public synchronized void close() {
    super.close();
    tabletList.clear();
    isTabletAlignedList.clear();
  }

  private List<Pair<String, File>> writeTabletsToTsFiles() throws WriteProcessException {
    final List<Pair<String, File>> sealedFiles = new ArrayList<>();
    try (final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(createFile())) {
      writeTabletsIntoOneFile(writer);
      sealedFiles.add(new Pair<>(null, writer.getFile()));
    } catch (final Exception e) {
      LOGGER.warn(
          "Batch id = {}: Failed to write tablets into tsfile, because {}",
          currentBatchId.get(),
          e.getMessage(),
          e);
      // TODO: handle ex
      throw new WriteProcessException(e);
    }

    return sealedFiles;
  }

  private void writeTabletsIntoOneFile(final RestorableTsFileIOWriter writer) throws Exception {
    // TODO: database & region
    final String database = "test_db";
    final String dataRegionId = "test_dr";
    final IMemTable memTable = new PrimitiveMemTable(database, dataRegionId);

    for (int i = 0, size = tabletList.size(); i < size; ++i) {
      final Tablet tablet = tabletList.get(i);

      // convert date value to int
      // refer to
      // org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunk.writeNonAlignedTablet
      final Object[] values = tablet.getValues();
      for (int j = 0; j < tablet.getSchemas().size(); ++j) {
        if (Objects.equals(TSDataType.DATE, tablet.getSchemas().get(j).getType())) {
          final LocalDate[] dates = ((LocalDate[]) values[j]);
          final int[] dateValues = new int[dates.length];
          for (int k = 0; k < Math.min(dates.length, tablet.getRowSize()); k++) {
            dateValues[k] = DateUtils.parseDateExpressionToInt(dates[k]);
          }
          values[j] = dateValues;
        }
      }

      final InsertTabletNode insertTabletNode =
          new InsertTabletNode(
              // TODO: plan node id
              new PlanNodeId("test_id"),
              new PartialPath(tablet.getDeviceId()),
              isTabletAlignedList.get(i),
              tablet.getSchemas().stream()
                  .map(IMeasurementSchema::getMeasurementName)
                  .toArray(String[]::new),
              tablet.getSchemas().stream()
                  .map(IMeasurementSchema::getType)
                  .toArray(TSDataType[]::new),
              // TODO: cast
              tablet.getSchemas().stream()
                  .map(schema -> (MeasurementSchema) schema)
                  .toArray(MeasurementSchema[]::new),
              tablet.getTimestamps(),
              tablet.getBitMaps(),
              tablet.getValues(),
              tablet.getRowSize());

      // TODO: unused results
      final TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);

      final int loc = insertTabletNode.checkTTL(results, getTTL(insertTabletNode));
      final List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs =
          insertTabletNode.splitByDevice(loc, insertTabletNode.getRowCount());
      int start = loc;
      final Map<Long, List<int[]>[]> splitInfo = new HashMap<>();
      for (final Pair<IDeviceID, Integer> deviceEndOffsetPair : deviceEndOffsetPairs) {
        final int end = deviceEndOffsetPair.getRight();
        split(insertTabletNode, start, end, splitInfo);
        start = end;
      }

      doInsert(insertTabletNode, splitInfo, results, memTable);
    }

    final MemTableFlushTask memTableFlushTask =
        new MemTableFlushTask(memTable, writer, database, dataRegionId);
    memTableFlushTask.syncFlushMemTable();

    writer.endFile();
  }

  private void split(
      final InsertTabletNode insertTabletNode,
      int loc,
      final int endOffset,
      final Map<Long, List<int[]>[]> splitInfo) {
    // before is first start point
    int before = loc;
    final long beforeTime = insertTabletNode.getTimes()[before];
    // before time partition
    long beforeTimePartition = TimePartitionUtils.getTimePartitionId(beforeTime);

    // if is sequence
    // TODO: always un-sequence now
    final boolean isSequence = false;
    while (loc < endOffset) {
      final long time = insertTabletNode.getTimes()[loc];
      final long timePartitionId = TimePartitionUtils.getTimePartitionId(time);

      // judge if we should insert sequence
      if (timePartitionId != beforeTimePartition) {
        updateSplitInfo(splitInfo, beforeTimePartition, isSequence, new int[] {before, loc});
        before = loc;
        beforeTimePartition = timePartitionId;
      }
      // else: the same partition and isSequence not changed, just move the cursor forward
      loc++;
    }

    // do not forget last part
    if (before < loc) {
      updateSplitInfo(splitInfo, beforeTimePartition, isSequence, new int[] {before, loc});
    }
  }

  private void updateSplitInfo(
      final Map<Long, List<int[]>[]> splitInfo,
      final long partitionId,
      final boolean isSequence,
      final int[] newRange) {
    if (newRange[0] >= newRange[1]) {
      return;
    }

    @SuppressWarnings("unchecked")
    final List<int[]>[] rangeLists = splitInfo.computeIfAbsent(partitionId, k -> new List[2]);
    List<int[]> rangeList = rangeLists[isSequence ? 1 : 0];
    if (rangeList == null) {
      rangeList = new ArrayList<>();
      rangeLists[isSequence ? 1 : 0] = rangeList;
    }
    if (!rangeList.isEmpty()) {
      final int[] lastRange = rangeList.get(rangeList.size() - 1);
      if (lastRange[1] == newRange[0]) {
        lastRange[1] = newRange[1];
        return;
      }
    }
    rangeList.add(newRange);
  }

  private void doInsert(
      final InsertTabletNode insertTabletNode,
      final Map<Long, List<int[]>[]> splitMap,
      final TSStatus[] results,
      final IMemTable memTable)
      throws WriteProcessException {
    for (final Entry<Long, List<int[]>[]> entry : splitMap.entrySet()) {
      final List<int[]>[] rangeLists = entry.getValue();
      final List<int[]> sequenceRangeList = rangeLists[1];
      if (sequenceRangeList != null) {
        insertTablet(insertTabletNode, sequenceRangeList, results, memTable);
      }
      final List<int[]> unSequenceRangeList = rangeLists[0];
      if (unSequenceRangeList != null) {
        insertTablet(insertTabletNode, unSequenceRangeList, results, memTable);
      }
    }
  }

  private void insertTablet(
      final InsertTabletNode insertTabletNode,
      final List<int[]> rangeList,
      final TSStatus[] results,
      final IMemTable memTable)
      throws WriteProcessException {
    for (final int[] rangePair : rangeList) {
      final int start = rangePair[0];
      final int end = rangePair[1];
      try {
        if (insertTabletNode.isAligned()) {
          memTable.insertAlignedTablet(insertTabletNode, start, end, null);
        } else {
          memTable.insertTablet(insertTabletNode, start, end);
        }
      } catch (final org.apache.iotdb.db.exception.WriteProcessException e) {
        for (int i = start; i < end; i++) {
          results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        throw new WriteProcessException(e);
      }
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.SUCCESS_STATUS;
      }
    }
  }

  private long getTTL(final InsertNode insertNode) {
    return DataNodeTTLCache.getInstance().getTTLForTree(insertNode.getTargetPath().getNodes());
  }
}
