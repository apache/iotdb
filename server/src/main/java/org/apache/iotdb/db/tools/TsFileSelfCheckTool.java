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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.exception.TsFileTimeseriesMetadataException;
import org.apache.iotdb.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSelfCheckTool {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSelfCheckTool.class);

  public long check(String filename, boolean fastFinish)
      throws IOException, TsFileStatisticsMistakesException, TsFileTimeseriesMetadataException {
    System.out.println("file path: " + filename);

    TsFileSelfCheckToolReader reader = new TsFileSelfCheckToolReader(filename);
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = null;
    try {
      timeseriesMetadataMap = reader.getAllTimeseriesMetadataWithOffset();
    } catch (Exception e) {
      logger.error("Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
      throw new TsFileTimeseriesMetadataException(e.getMessage());
    }
    return reader.selfCheckWithInfo(filename, fastFinish, timeseriesMetadataMap);
  }

  public Map<Long, Pair<Path, TimeseriesMetadata>> getTimeseriesMetadataMap(String filename)
      throws IOException, TsFileTimeseriesMetadataException {
    TsFileSelfCheckToolReader reader = new TsFileSelfCheckToolReader(filename);
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = null;
    try {
      timeseriesMetadataMap = reader.getAllTimeseriesMetadataWithOffset();
    } catch (Exception e) {
      logger.error("Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
      throw new TsFileTimeseriesMetadataException(e.getMessage());
    }
    return timeseriesMetadataMap;
  }

  private class TsFileSelfCheckToolReader extends TsFileSequenceReader {
    public TsFileSelfCheckToolReader(String file) throws IOException {
      super(file);
    }

    /**
     * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
     *
     * @param metadataIndex MetadataIndexEntry
     * @param buffer byte buffer
     * @param deviceId String
     * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
     * @param needChunkMetadata deserialize chunk metadata list or not
     */
    private void generateMetadataIndexWithOffset(
        long startOffset,
        MetadataIndexEntry metadataIndex,
        ByteBuffer buffer,
        String deviceId,
        MetadataIndexNodeType type,
        Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap,
        boolean needChunkMetadata)
        throws IOException {
      try {
        if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          while (buffer.hasRemaining()) {
            long pos = startOffset + buffer.position();
            TimeseriesMetadata timeseriesMetadata =
                TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata);
            timeseriesMetadataMap.put(
                pos,
                new Pair<>(
                    new Path(deviceId, timeseriesMetadata.getMeasurementId()), timeseriesMetadata));
          }
        } else {
          // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = metadataIndex.getName();
          }
          MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
          int metadataIndexListSize = metadataIndexNode.getChildren().size();
          for (int i = 0; i < metadataIndexListSize; i++) {
            long endOffset = metadataIndexNode.getEndOffset();
            if (i != metadataIndexListSize - 1) {
              endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
            }
            ByteBuffer nextBuffer =
                readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
            generateMetadataIndexWithOffset(
                metadataIndexNode.getChildren().get(i).getOffset(),
                metadataIndexNode.getChildren().get(i),
                nextBuffer,
                deviceId,
                metadataIndexNode.getNodeType(),
                timeseriesMetadataMap,
                needChunkMetadata);
          }
        }
      } catch (BufferOverflowException e) {
        throw e;
      }
    }

    public Map<Long, Pair<Path, TimeseriesMetadata>> getAllTimeseriesMetadataWithOffset()
        throws IOException {
      if (tsFileMetaData == null) {
        readFileMetadata();
      }
      MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
        long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
        if (i != metadataIndexEntryList.size() - 1) {
          endOffset = metadataIndexEntryList.get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
        generateMetadataIndexWithOffset(
            metadataIndexEntry.getOffset(),
            metadataIndexEntry,
            buffer,
            null,
            metadataIndexNode.getNodeType(),
            timeseriesMetadataMap,
            false);
      }
      return timeseriesMetadataMap;
    }
  }
}
