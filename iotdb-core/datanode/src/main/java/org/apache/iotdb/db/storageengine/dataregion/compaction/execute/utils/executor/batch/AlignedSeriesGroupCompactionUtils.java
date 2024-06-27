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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlignedSeriesGroupCompactionUtils {

  private AlignedSeriesGroupCompactionUtils() {}

  public static List<IMeasurementSchema> selectColumnGroupToCompact(
      List<IMeasurementSchema> schemaList, Set<String> compactedMeasurements) {
    int compactColumnNum =
        IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
    List<IMeasurementSchema> selectedColumnGroup = new ArrayList<>(compactColumnNum);
    for (IMeasurementSchema schema : schemaList) {
      if (!schema.getType().equals(TSDataType.TEXT)) {
        continue;
      }
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      compactedMeasurements.add(schema.getMeasurementId());
      selectedColumnGroup.add(schema);
      if (compactedMeasurements.size() == schemaList.size()) {
        return selectedColumnGroup;
      }
    }
    for (IMeasurementSchema schema : schemaList) {
      if (compactedMeasurements.contains(schema.getMeasurementId())) {
        continue;
      }
      selectedColumnGroup.add(schema);
      compactedMeasurements.add(schema.getMeasurementId());
      if (selectedColumnGroup.size() == compactColumnNum) {
        break;
      }
      if (compactedMeasurements.size() == schemaList.size()) {
        break;
      }
    }
    return selectedColumnGroup;
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
      if (measurementIndex.containsKey(chunkMetadata.getMeasurementUid())) {
        valueChunkMetadataList.set(
            measurementIndex.get(chunkMetadata.getMeasurementUid()), chunkMetadata);
      }
    }
    return new AlignedChunkMetadata(
        alignedChunkMetadata.getTimeChunkMetadata(), valueChunkMetadataList);
  }
}
