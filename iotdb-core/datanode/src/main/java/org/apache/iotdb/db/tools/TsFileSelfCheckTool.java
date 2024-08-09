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

import org.apache.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSelfCheckTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSelfCheckTool.class);

  private Map<Long, Pair<Path, TimeseriesMetadata>> getTimeseriesMetadataMapWithReader(
      TsFileSelfCheckToolReader reader) throws Exception {
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = null;
    timeseriesMetadataMap = reader.getAllTimeseriesMetadataWithOffset();
    return timeseriesMetadataMap;
  }

  public Map<Long, Pair<Path, TimeseriesMetadata>> getTimeseriesMetadataMapWithPath(String filename)
      throws IOException, TsFileTimeseriesMetadataException {
    TsFileSelfCheckToolReader reader = new TsFileSelfCheckToolReader(filename);
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = null;
    try {
      timeseriesMetadataMap = getTimeseriesMetadataMapWithReader(reader);
    } catch (Exception e) {
      LOGGER.error("Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
      throw new TsFileTimeseriesMetadataException(
          "Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
    }
    return timeseriesMetadataMap;
  }

  /**
   * @param filename The path of TsFile.
   * @param fastFinish If true, the method will only check the format of head (Magic String TsFile,
   *     Version Number) and tail (Magic String TsFile) of TsFile.
   * @return There are four return values of the check method. The return value is 0, which means
   *     that the TsFile self check is error-free. The return value is -1, which means that TsFile
   *     has inconsistencies in Statistics. There will be two specific exceptions, one is that the
   *     Statistics of TimeSeriesMetadata is inconsistent with the Statistics of the aggregated
   *     statistics of ChunkMetadata. The other is that the Statistics of ChunkMetadata is
   *     inconsistent with the Statistics of Page aggregation statistics in the Chunk indexed by it.
   *     The return value is -2, which means that the TsFile version is not compatible. The return
   *     value is -3, which means that the TsFile file does not exist in the given path.
   */
  public long check(String filename, boolean fastFinish)
      throws IOException, TsFileStatisticsMistakesException, TsFileTimeseriesMetadataException {
    LOGGER.info("file path: {}", filename);
    TsFileSelfCheckToolReader reader = new TsFileSelfCheckToolReader(filename);
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = null;
    long res = -1;
    try {
      timeseriesMetadataMap = getTimeseriesMetadataMapWithReader(reader);
      res = reader.selfCheckWithInfo(filename, fastFinish, timeseriesMetadataMap);
    } catch (TsFileStatisticsMistakesException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
      throw new TsFileTimeseriesMetadataException(
          "Error occurred while getting all TimeseriesMetadata with offset in TsFile.");
    } finally {
      reader.close();
    }
    return res;
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
        IMetadataIndexEntry metadataIndex,
        ByteBuffer buffer,
        IDeviceID deviceId,
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
                    new Path(deviceId, timeseriesMetadata.getMeasurementId(), true),
                    timeseriesMetadata));
          }
        } else {
          // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
          }
          boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
          MetadataIndexNode metadataIndexNode =
              getDeserializeContext()
                  .deserializeMetadataIndexNode(buffer, currentChildLevelIsDevice);
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
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      for (MetadataIndexNode metadataIndexNode :
          tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
        List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
        for (int i = 0; i < metadataIndexEntryList.size(); i++) {
          IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
          long endOffset = metadataIndexNode.getEndOffset();
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
      }

      return timeseriesMetadataMap;
    }
  }
}
