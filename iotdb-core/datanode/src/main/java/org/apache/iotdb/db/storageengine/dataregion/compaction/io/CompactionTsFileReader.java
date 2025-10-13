/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class extends the TsFileSequenceReader class to read and manage TsFile with a focus on
 * compaction-related operations. This includes functions for tracking and recording the amount of
 * data read and distinguishing between aligned and not aligned series during compaction.
 */
public class CompactionTsFileReader extends TsFileSequenceReader {
  @TestOnly
  public CompactionTsFileReader(String file, CompactionType compactionType) throws IOException {
    super(file, new EncryptParameter(config.getEncryptType(), config.getEncryptKey()));
    CompactionTsFileInput compactionTsFileInput =
        new CompactionTsFileInput(compactionType, tsFileInput);
    this.tsFileInput = compactionTsFileInput;
    compactionTsFileInput.setMetadataOffset(readFileMetadata().getMetaOffset());
  }

  /**
   * Constructs a new instance of CompactionTsFileReader.
   *
   * @param file The file to be read.
   * @param compactionType The type of compaction running.
   * @param encryptParameter The first encryption parameters for the file.
   * @throws IOException If an error occurs during file operations.
   */
  public CompactionTsFileReader(
      String file, CompactionType compactionType, EncryptParameter encryptParameter)
      throws IOException {
    super(file, encryptParameter);
    CompactionTsFileInput compactionTsFileInput =
        new CompactionTsFileInput(compactionType, tsFileInput);
    this.tsFileInput = compactionTsFileInput;
    compactionTsFileInput.setMetadataOffset(readFileMetadata().getMetaOffset());
  }

  /** Marks the start of reading an aligned series. */
  public void markStartOfAlignedSeries() {
    ((CompactionTsFileInput) tsFileInput).markStartOfAlignedSeries();
  }

  /** Marks the end of reading an aligned series. */
  public void markEndOfAlignedSeries() {
    ((CompactionTsFileInput) tsFileInput).markEndOfAlignedSeries();
  }

  @SuppressWarnings("java:S2177")
  public ChunkHeader readChunkHeader(long position) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position);
  }

  public Map<String, Pair<TimeseriesMetadata, Pair<Long, Long>>>
      getTimeseriesMetadataAndOffsetByDevice(
          MetadataIndexNode measurementNode,
          Set<String> excludedMeasurementIds,
          boolean needChunkMetadata)
          throws IOException {
    Map<String, Pair<TimeseriesMetadata, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new LinkedHashMap<>();
    List<IMetadataIndexEntry> childrenEntryList = measurementNode.getChildren();
    for (int i = 0; i < childrenEntryList.size(); i++) {
      long startOffset = childrenEntryList.get(i).getOffset();
      long endOffset =
          i == childrenEntryList.size() - 1
              ? measurementNode.getEndOffset()
              : childrenEntryList.get(i + 1).getOffset();
      ByteBuffer nextBuffer = readData(startOffset, endOffset);
      if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        // leaf measurement node
        while (nextBuffer.hasRemaining()) {
          int metadataStartOffset = nextBuffer.position();
          TimeseriesMetadata timeseriesMetadata =
              TimeseriesMetadata.deserializeFrom(
                  nextBuffer, excludedMeasurementIds, needChunkMetadata);
          timeseriesMetadataOffsetMap.put(
              timeseriesMetadata.getMeasurementId(),
              new Pair<>(
                  timeseriesMetadata,
                  new Pair<>(
                      startOffset + metadataStartOffset, startOffset + nextBuffer.position())));
        }

      } else {
        // internal measurement node
        MetadataIndexNode nextLayerMeasurementNode =
            getDeserializeContext().deserializeMetadataIndexNode(nextBuffer, false);
        timeseriesMetadataOffsetMap.putAll(
            getTimeseriesMetadataAndOffsetByDevice(
                nextLayerMeasurementNode, excludedMeasurementIds, needChunkMetadata));
      }
    }
    return timeseriesMetadataOffsetMap;
  }

  public Map<String, Pair<Long, Long>> getTimeseriesMetadataOffsetByDevice(
      MetadataIndexNode measurementNode) throws IOException {
    Map<String, Pair<Long, Long>> timeseriesMetadataOffsetMap = new LinkedHashMap<>();
    List<IMetadataIndexEntry> childrenEntryList = measurementNode.getChildren();
    for (int i = 0; i < childrenEntryList.size(); i++) {
      long startOffset = childrenEntryList.get(i).getOffset();
      long endOffset =
          i == childrenEntryList.size() - 1
              ? measurementNode.getEndOffset()
              : childrenEntryList.get(i + 1).getOffset();
      if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        // leaf measurement node
        timeseriesMetadataOffsetMap.put(
            childrenEntryList.get(i).getCompareKey().toString(),
            new Pair<>(startOffset, endOffset));
      } else {
        // internal measurement node
        ByteBuffer nextBuffer = readData(startOffset, endOffset);
        MetadataIndexNode nextLayerMeasurementNode =
            getDeserializeContext().deserializeMetadataIndexNode(nextBuffer, false);
        timeseriesMetadataOffsetMap.putAll(
            getTimeseriesMetadataOffsetByDevice(nextLayerMeasurementNode));
      }
    }
    return timeseriesMetadataOffsetMap;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
