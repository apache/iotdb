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

package org.apache.tsfile.read;

import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.exception.TsFileSequenceReaderTimeseriesMetadataIteratorException;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TsFileSequenceReaderTimeseriesMetadataIterator
    implements Iterator<Map<IDeviceID, List<TimeseriesMetadata>>> {

  private static final int DEFAULT_TIMESERIES_BATCH_READ_NUMBER = 4000;
  private final TsFileSequenceReader reader;
  private final boolean needChunkMetadata;
  private final int timeseriesBatchReadNumber;
  private ByteBuffer currentBuffer = null;
  private long currentEndOffset = Long.MIN_VALUE;
  private final Deque<MetadataIndexEntryInfo> metadataIndexEntryStack = new ArrayDeque<>();
  private IDeviceID currentDeviceId;
  private int currentTimeseriesMetadataCount = 0;
  private DeserializeConfig deserializeConfig;

  public TsFileSequenceReaderTimeseriesMetadataIterator(
      TsFileSequenceReader reader, boolean needChunkMetadata, int timeseriesBatchReadNumber)
      throws IOException {
    this.reader = reader;
    this.needChunkMetadata = needChunkMetadata;
    this.timeseriesBatchReadNumber = timeseriesBatchReadNumber;
    this.deserializeConfig = this.reader.getDeserializeContext();

    if (this.reader.tsFileMetaData == null) {
      this.reader.readFileMetadata();
    }

    for (MetadataIndexNode metadataIndexNode :
        reader.tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
      long curEntryEndOffset = metadataIndexNode.getEndOffset();
      List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();

      for (int i = metadataIndexEntryList.size() - 1; i >= 0; i--) {
        metadataIndexEntryStack.push(
            new MetadataIndexEntryInfo(
                metadataIndexEntryList.get(i), metadataIndexNode.getNodeType(), curEntryEndOffset));
        curEntryEndOffset = metadataIndexEntryList.get(i).getOffset();
      }
    }
  }

  public TsFileSequenceReaderTimeseriesMetadataIterator(
      TsFileSequenceReader reader, boolean needChunkMetadata) throws IOException {
    this(reader, needChunkMetadata, DEFAULT_TIMESERIES_BATCH_READ_NUMBER);
  }

  @Override
  public boolean hasNext() {
    return !metadataIndexEntryStack.isEmpty()
        || (currentBuffer != null && currentBuffer.hasRemaining());
  }

  @Override
  public Map<IDeviceID, List<TimeseriesMetadata>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();

    while (currentTimeseriesMetadataCount < timeseriesBatchReadNumber) {
      // 1. Check Buffer
      // currentTimeseriesMetadataCount has reached the limit in the previous
      // loop and maybe there is still some data that remains in the buffer.
      if (currentBuffer != null && currentBuffer.hasRemaining()) {
        timeseriesMetadataMap
            .computeIfAbsent(currentDeviceId, k -> new ArrayList<>())
            .addAll(deserializeTimeseriesMetadata());
      } else if (currentEndOffset > Long.MIN_VALUE) {
        try {
          timeseriesMetadataMap
              .computeIfAbsent(currentDeviceId, k -> new ArrayList<>())
              .addAll(deserializeTimeseriesMetadataUsingTsFileInput(currentEndOffset));
        } catch (IOException e) {
          throw new TsFileSequenceReaderTimeseriesMetadataIteratorException(
              String.format(
                  "TsFileSequenceReaderTimeseriesMetadataIterator: deserializeTimeseriesMetadataUsingTsFileInput failed, "
                      + "currentEndOffset: %d, "
                      + e.getMessage(),
                  currentEndOffset));
        }
      }

      if (currentTimeseriesMetadataCount >= timeseriesBatchReadNumber
          || metadataIndexEntryStack.isEmpty()) {
        break;
      }

      // 2. Deserialize MetadataIndexEntry
      final MetadataIndexEntryInfo indexEntryInfo = metadataIndexEntryStack.pop();

      try {
        deserializeMetadataIndexEntry(indexEntryInfo, timeseriesMetadataMap);
      } catch (IOException e) {
        throw new TsFileSequenceReaderTimeseriesMetadataIteratorException(
            String.format(
                "TsFileSequenceReaderTimeseriesMetadataIterator: deserializeMetadataIndexEntry failed, "
                    + "MetadataIndexEntryInfo: %s, "
                    + e.getMessage(),
                indexEntryInfo));
      }
    }

    // 3. Reset currentTimeseriesMetadataCount
    if (currentTimeseriesMetadataCount >= timeseriesBatchReadNumber) {
      currentTimeseriesMetadataCount = 0;
    }

    return timeseriesMetadataMap;
  }

  private void deserializeMetadataIndexEntry(
      MetadataIndexEntryInfo metadataIndexEntryInfo,
      Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap)
      throws IOException {
    if (metadataIndexEntryInfo
        .getMetadataIndexNodeType()
        .equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      deserializeLeafMeasurement(
          metadataIndexEntryInfo.getMetadataIndexEntry(),
          metadataIndexEntryInfo.getEndOffset(),
          timeseriesMetadataMap);

    } else {
      deserializeInternalNode(
          metadataIndexEntryInfo.getMetadataIndexEntry(),
          metadataIndexEntryInfo.getEndOffset(),
          metadataIndexEntryInfo.getMetadataIndexNodeType());
    }
  }

  private void deserializeLeafMeasurement(
      IMetadataIndexEntry metadataIndexEntry,
      long endOffset,
      Map<IDeviceID, List<TimeseriesMetadata>> timeseriesMetadataMap)
      throws IOException {
    if (currentBuffer != null && currentBuffer.hasRemaining()) {
      throw new TsFileSequenceReaderTimeseriesMetadataIteratorException(
          "currentBuffer still has some data left before deserializeLeafMeasurement");
    }
    if (endOffset - metadataIndexEntry.getOffset() < Integer.MAX_VALUE) {
      currentBuffer = reader.readData(metadataIndexEntry.getOffset(), endOffset);
      timeseriesMetadataMap
          .computeIfAbsent(currentDeviceId, k -> new ArrayList<>())
          .addAll(deserializeTimeseriesMetadata());
    } else {
      currentEndOffset = endOffset;
      reader.position(metadataIndexEntry.getOffset());
      timeseriesMetadataMap
          .computeIfAbsent(currentDeviceId, k -> new ArrayList<>())
          .addAll(deserializeTimeseriesMetadataUsingTsFileInput(endOffset));
    }
  }

  private List<TimeseriesMetadata> deserializeTimeseriesMetadata() {
    final List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    while (currentBuffer.hasRemaining()
        && currentTimeseriesMetadataCount < timeseriesBatchReadNumber) {
      timeseriesMetadataList.add(
          TimeseriesMetadata.deserializeFrom(currentBuffer, needChunkMetadata));
      currentTimeseriesMetadataCount++;
    }
    return timeseriesMetadataList;
  }

  private List<TimeseriesMetadata> deserializeTimeseriesMetadataUsingTsFileInput(long endOffset)
      throws IOException {
    final List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    while (reader.position() < endOffset
        && currentTimeseriesMetadataCount < DEFAULT_TIMESERIES_BATCH_READ_NUMBER) {
      timeseriesMetadataList.add(
          TimeseriesMetadata.deserializeFrom(reader.tsFileInput, needChunkMetadata));
      currentTimeseriesMetadataCount++;
    }
    if (reader.position() >= endOffset) {
      currentEndOffset = Long.MIN_VALUE;
    }
    return timeseriesMetadataList;
  }

  private void deserializeInternalNode(
      IMetadataIndexEntry metadataIndexEntry, long endOffset, MetadataIndexNodeType type)
      throws IOException {
    if (MetadataIndexNodeType.LEAF_DEVICE.equals(type)) {
      currentDeviceId = ((DeviceMetadataIndexEntry) metadataIndexEntry).getDeviceID();
    }

    boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
    final MetadataIndexNode metadataIndexNode =
        deserializeConfig.deserializeMetadataIndexNode(
            reader.readData(metadataIndexEntry.getOffset(), endOffset), currentChildLevelIsDevice);
    MetadataIndexNodeType metadataIndexNodeType = metadataIndexNode.getNodeType();
    List<IMetadataIndexEntry> children = metadataIndexNode.getChildren();
    long curEntryEndOffset = metadataIndexNode.getEndOffset();

    for (int i = children.size() - 1; i >= 0; i--) {
      metadataIndexEntryStack.push(
          new MetadataIndexEntryInfo(children.get(i), metadataIndexNodeType, curEntryEndOffset));
      curEntryEndOffset = children.get(i).getOffset();
    }
  }

  private static class MetadataIndexEntryInfo {
    private final IMetadataIndexEntry metadataIndexEntry;
    private final MetadataIndexNodeType metadataIndexNodeType;
    private final long endOffset;

    public MetadataIndexEntryInfo(
        IMetadataIndexEntry metadataIndexEntry,
        MetadataIndexNodeType metadataIndexNodeType,
        long endOffset) {
      this.metadataIndexEntry = metadataIndexEntry;
      this.metadataIndexNodeType = metadataIndexNodeType;
      this.endOffset = endOffset;
    }

    public IMetadataIndexEntry getMetadataIndexEntry() {
      return metadataIndexEntry;
    }

    public MetadataIndexNodeType getMetadataIndexNodeType() {
      return metadataIndexNodeType;
    }

    public long getEndOffset() {
      return endOffset;
    }
  }
}
