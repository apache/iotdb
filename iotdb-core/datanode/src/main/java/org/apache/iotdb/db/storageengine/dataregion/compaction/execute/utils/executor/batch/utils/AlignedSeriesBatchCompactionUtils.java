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
import java.util.Set;

public class AlignedSeriesBatchCompactionUtils {

  private AlignedSeriesBatchCompactionUtils() {}

  public static List<IMeasurementSchema> selectColumnBatchToCompact(
      List<IMeasurementSchema> schemaList, Set<String> compactedMeasurements, int batchSize) {
    // TODO: select batch by allocated memory and chunk size to perform more strict memory control
    List<IMeasurementSchema> selectedColumnBatch = new ArrayList<>(batchSize);
    for (IMeasurementSchema schema : schemaList) {
      if (!isLargeDataType(schema.getType())) {
        continue;
      }
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      compactedMeasurements.add(schema.getMeasurementId());
      selectedColumnBatch.add(schema);
      if (selectedColumnBatch.size() >= batchSize) {
        return selectedColumnBatch;
      }
      if (compactedMeasurements.size() == schemaList.size()) {
        return selectedColumnBatch;
      }
    }
    for (IMeasurementSchema schema : schemaList) {
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      selectedColumnBatch.add(schema);
      compactedMeasurements.add(schema.getMeasurementId());
      if (selectedColumnBatch.size() >= batchSize) {
        break;
      }
      if (compactedMeasurements.size() == schemaList.size()) {
        break;
      }
    }
    return selectedColumnBatch;
  }

  private static boolean isLargeDataType(TSDataType dataType) {
    return dataType.equals(TSDataType.BLOB)
        || dataType.equals(TSDataType.TEXT)
        || dataType.equals(TSDataType.STRING);
  }

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
}
