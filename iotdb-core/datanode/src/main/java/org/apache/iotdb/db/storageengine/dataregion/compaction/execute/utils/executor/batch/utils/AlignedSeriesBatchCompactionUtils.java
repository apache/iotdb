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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class AlignedSeriesBatchCompactionUtils {

  private AlignedSeriesBatchCompactionUtils() {}

  public static void markAlignedChunkHasDeletion(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          readerAndChunkMetadataList) {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair : readerAndChunkMetadataList) {
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      markAlignedChunkHasDeletion(alignedChunkMetadataList);
    }
  }

  public static void markAlignedChunkHasDeletion(
      List<AlignedChunkMetadata> alignedChunkMetadataList) {
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
      for (IChunkMetadata iChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        if (iChunkMetadata != null && iChunkMetadata.isModified()) {
          timeChunkMetadata.setModified(true);
          break;
        }
      }
    }
  }

  public static AlignedChunkMetadata filterAlignedChunkMetadataByIndex(
      AlignedChunkMetadata alignedChunkMetadata, List<Integer> selectedMeasurements) {
    IChunkMetadata[] valueChunkMetadataArr = new IChunkMetadata[selectedMeasurements.size()];
    List<IChunkMetadata> originValueChunkMetadataList =
        alignedChunkMetadata.getValueChunkMetadataList();
    for (int i = 0; i < selectedMeasurements.size(); i++) {
      int columnIndex = selectedMeasurements.get(i);
      valueChunkMetadataArr[i] = originValueChunkMetadataList.get(columnIndex);
    }
    return new AlignedChunkMetadata(
        alignedChunkMetadata.getTimeChunkMetadata(), Arrays.asList(valueChunkMetadataArr));
  }

  public static AlignedChunkMetadata filterAlignedChunkMetadata(
      AlignedChunkMetadata alignedChunkMetadata, List<String> selectedMeasurements) {
    List<IChunkMetadata> valueChunkMetadataList =
        Arrays.asList(new IChunkMetadata[selectedMeasurements.size()]);

    Map<String, Integer> measurementIndex = new HashMap<>();
    for (int i = 0; i < selectedMeasurements.size(); i++) {
      measurementIndex.put(selectedMeasurements.get(i), i);
    }

    for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (chunkMetadata == null) {
        continue;
      }
      Integer idx = measurementIndex.get(chunkMetadata.getMeasurementUid());
      if (idx != null) {
        valueChunkMetadataList.set(idx, chunkMetadata);
      }
    }
    return new AlignedChunkMetadata(
        alignedChunkMetadata.getTimeChunkMetadata(), valueChunkMetadataList);
  }

  public static AlignedChunkMetadata fillAlignedChunkMetadataBySchemaList(
      AlignedChunkMetadata originAlignedChunkMetadata, List<IMeasurementSchema> schemaList) {
    List<IChunkMetadata> originValueChunkMetadataList =
        originAlignedChunkMetadata.getValueChunkMetadataList();
    if (originValueChunkMetadataList.size() == schemaList.size()) {
      return originAlignedChunkMetadata;
    }
    IChunkMetadata[] newValueChunkMetadataArr = new IChunkMetadata[schemaList.size()];
    int currentValueChunkMetadataIndex = 0;
    for (int i = 0; i < schemaList.size(); i++) {
      IMeasurementSchema currentSchema = schemaList.get(i);
      if (currentValueChunkMetadataIndex >= originValueChunkMetadataList.size()) {
        break;
      }
      IChunkMetadata currentValueChunkMetadata =
          originValueChunkMetadataList.get(currentValueChunkMetadataIndex);
      if (currentValueChunkMetadata != null
          && currentSchema
              .getMeasurementId()
              .equals(currentValueChunkMetadata.getMeasurementUid())) {
        newValueChunkMetadataArr[i] = currentValueChunkMetadata;
        currentValueChunkMetadataIndex++;
      }
    }
    return new AlignedChunkMetadata(
        originAlignedChunkMetadata.getTimeChunkMetadata(), Arrays.asList(newValueChunkMetadataArr));
  }

  public static ModifiedStatus calculateAlignedPageModifiedStatus(
      long startTime, long endTime, AlignedChunkMetadata originAlignedChunkMetadata) {
    ModifiedStatus lastPageStatus = null;
    for (IChunkMetadata valueChunkMetadata :
        originAlignedChunkMetadata.getValueChunkMetadataList()) {
      ModifiedStatus currentPageStatus =
          valueChunkMetadata == null
              ? ModifiedStatus.ALL_DELETED
              : checkIsModified(startTime, endTime, valueChunkMetadata.getDeleteIntervalList());
      if (currentPageStatus == ModifiedStatus.PARTIAL_DELETED) {
        // one of the value pages exist data been deleted partially
        return ModifiedStatus.PARTIAL_DELETED;
      }
      if (lastPageStatus == null) {
        // first page
        lastPageStatus = currentPageStatus;
        continue;
      }
      if (!lastPageStatus.equals(currentPageStatus)) {
        // there are at least two value pages, one is that all data is deleted, the other is that no
        // data is deleted
        lastPageStatus = ModifiedStatus.NONE_DELETED;
      }
    }
    return lastPageStatus;
  }

  public static ModifiedStatus checkIsModified(
      long startTime, long endTime, Collection<TimeRange> deletions) {
    ModifiedStatus status = ModifiedStatus.NONE_DELETED;
    if (deletions != null) {
      for (TimeRange range : deletions) {
        if (range.contains(startTime, endTime)) {
          // all data on this page or chunk has been deleted
          return ModifiedStatus.ALL_DELETED;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          // exist data on this page or chunk been deleted
          status = ModifiedStatus.PARTIAL_DELETED;
        }
      }
    }
    return status;
  }

  public static class BatchColumnSelection {
    private final List<IMeasurementSchema> schemaList;
    private final LinkedList<Integer> typeSortedColumnIndexList;
    private final LinkedList<Integer> largeTypeSortedColumnIndexList;
    private final int batchSize;
    private int selectedColumnNum;

    private List<Integer> columnIndexList;
    private List<IMeasurementSchema> currentSelectedColumnSchemaList;

    public BatchColumnSelection(List<IMeasurementSchema> valueSchemas, int batchSize) {
      this.schemaList = valueSchemas;
      this.typeSortedColumnIndexList = new LinkedList<>();
      this.largeTypeSortedColumnIndexList = new LinkedList<>();
      for (int i = 0; i < valueSchemas.size(); i++) {
        IMeasurementSchema schema = valueSchemas.get(i);
        if (isLargeDataType(schema.getTypeInByte())) {
          this.largeTypeSortedColumnIndexList.addLast(i);
        } else {
          this.typeSortedColumnIndexList.addLast(i);
        }
      }
      this.batchSize = batchSize;
      this.selectedColumnNum = 0;
    }

    public boolean hasNext() {
      return selectedColumnNum < schemaList.size();
    }

    public void next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      selectColumnBatchToCompact();
    }

    public List<Integer> getSelectedColumnIndexList() {
      return this.columnIndexList;
    }

    public List<IMeasurementSchema> getCurrentSelectedColumnSchemaList() {
      return this.currentSelectedColumnSchemaList;
    }

    private void selectColumnBatchToCompact() {
      // TODO: select batch by allocated memory and chunk size to perform more strict memory control
      this.columnIndexList = new ArrayList<>(batchSize);
      this.currentSelectedColumnSchemaList = new ArrayList<>(batchSize);
      while (!typeSortedColumnIndexList.isEmpty() || !largeTypeSortedColumnIndexList.isEmpty()) {
        LinkedList<Integer> sourceTypeSortedList =
            largeTypeSortedColumnIndexList.isEmpty()
                ? typeSortedColumnIndexList
                : largeTypeSortedColumnIndexList;
        Integer columnIndex = sourceTypeSortedList.removeFirst();
        this.columnIndexList.add(columnIndex);
        this.currentSelectedColumnSchemaList.add(this.schemaList.get(columnIndex));
        selectedColumnNum++;
        if (this.columnIndexList.size() >= batchSize) {
          break;
        }
      }
    }

    private static boolean isLargeDataType(byte dataType) {
      return dataType == TSDataType.TEXT.getType()
          || dataType == TSDataType.STRING.getType()
          || dataType == TSDataType.BLOB.getType();
    }
  }
}
