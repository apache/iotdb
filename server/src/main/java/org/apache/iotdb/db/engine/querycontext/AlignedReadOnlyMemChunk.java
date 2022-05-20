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

package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.reader.chunk.MemAlignedChunkLoader;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {

  // deletion list for this chunk
  private final List<List<TimeRange>> deletionList;

  private String measurementUid;
  private TSDataType dataType;
  private List<TSEncoding> encodingList;

  private static final Logger logger = LoggerFactory.getLogger(AlignedReadOnlyMemChunk.class);

  private int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

  private AlignedTVList chunkData;

  private int chunkDataSize;

  /**
   * The constructor for Aligned type.
   *
   * @param schema VectorMeasurementSchema
   * @param tvList VectorTvList
   * @param size The Number of Chunk data points
   * @param deletionList The timeRange of deletionList
   */
  public AlignedReadOnlyMemChunk(
      IMeasurementSchema schema, TVList tvList, int size, List<List<TimeRange>> deletionList)
      throws IOException, QueryProcessException {
    super();
    this.measurementUid = schema.getMeasurementId();
    this.dataType = schema.getType();

    this.encodingList = ((VectorMeasurementSchema) schema).getSubMeasurementsTSEncodingList();
    this.chunkData = (AlignedTVList) tvList;
    this.chunkDataSize = size;
    this.deletionList = deletionList;

    this.chunkPointReader =
        (chunkData).getAlignedIterator(floatPrecision, encodingList, chunkDataSize, deletionList);
    initAlignedChunkMeta((VectorMeasurementSchema) schema);
  }

  private void initAlignedChunkMeta(VectorMeasurementSchema schema)
      throws IOException, QueryProcessException {
    AlignedTVList alignedChunkData = (AlignedTVList) chunkData;
    List<String> measurementList = schema.getSubMeasurementsList();
    List<TSDataType> dataTypeList = schema.getSubMeasurementsTSDataTypeList();
    // time chunk
    Statistics timeStatistics = Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(measurementUid, TSDataType.VECTOR, 0, timeStatistics);
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    // update time chunk
    boolean[] timeDuplicateInfo = null;
    for (int row = 0; row < alignedChunkData.rowCount(); row++) {
      if (row == alignedChunkData.rowCount() - 1
          || alignedChunkData.getTime(row) != alignedChunkData.getTime(row + 1)) {
        timeStatistics.update(alignedChunkData.getTime(row));
      } else {
        if (timeDuplicateInfo == null) {
          timeDuplicateInfo = new boolean[alignedChunkData.rowCount()];
        }
        timeDuplicateInfo[row] = true;
      }
    }
    timeStatistics.setEmpty(false);
    // update value chunk
    for (int column = 0; column < measurementList.size(); column++) {
      // Pair of Time and Index
      Pair<Long, Integer> lastValidPointIndexForTimeDupCheck = null;
      if (timeDuplicateInfo != null) {
        lastValidPointIndexForTimeDupCheck = new Pair<>(Long.MIN_VALUE, null);
      }
      Statistics valueStatistics = Statistics.getStatsByType(dataTypeList.get(column));
      IChunkMetadata valueChunkMetadata =
          new ChunkMetadata(
              measurementList.get(column), dataTypeList.get(column), 0, valueStatistics);
      valueChunkMetadataList.add(valueChunkMetadata);
      if (alignedChunkData.getValues().get(column) == null) {
        valueStatistics.setEmpty(true);
        continue;
      }
      for (int row = 0; row < alignedChunkData.rowCount(); row++) {
        long time = alignedChunkData.getTime(row);
        // skip time duplicated rows
        if (timeDuplicateInfo != null) {
          if (!alignedChunkData.isValueMarked(alignedChunkData.getValueIndex(row), column)) {
            lastValidPointIndexForTimeDupCheck.left = time;
            lastValidPointIndexForTimeDupCheck.right = alignedChunkData.getValueIndex(row);
          }
          if (timeDuplicateInfo[row]) {
            continue;
          }
        }
        // The part of code solves the following problem:
        // Time: 1,2,2,3
        // Value: 1,2,null,null
        // When rowIndex:1, pair(min,null), timeDuplicateInfo:false, write(T:1,V:1)
        // When rowIndex:2, pair(2,2), timeDuplicateInfo:true, skip writing value
        // When rowIndex:3, pair(2,2), timeDuplicateInfo:false, T:2!=air.left:2, write(T:2,V:2)
        // When rowIndex:4, pair(2,2), timeDuplicateInfo:false, T:3!=pair.left:2, write(T:3,V:null)
        int originRowIndex;
        if (lastValidPointIndexForTimeDupCheck != null
            && (alignedChunkData.getTime(row) == lastValidPointIndexForTimeDupCheck.left)) {
          originRowIndex = lastValidPointIndexForTimeDupCheck.right;
        } else {
          originRowIndex = alignedChunkData.getValueIndex(row);
        }
        boolean isNull = alignedChunkData.isValueMarked(originRowIndex, column);
        if (isNull) {
          continue;
        }
        switch (dataTypeList.get(column)) {
          case BOOLEAN:
            valueStatistics.update(
                time, alignedChunkData.getBooleanByValueIndex(originRowIndex, column));
            break;
          case TEXT:
            valueStatistics.update(
                time, alignedChunkData.getBinaryByValueIndex(originRowIndex, column));
            break;
          case FLOAT:
            valueStatistics.update(
                time, alignedChunkData.getFloatByValueIndex(originRowIndex, column));
            break;
          case INT32:
            valueStatistics.update(
                time, alignedChunkData.getIntByValueIndex(originRowIndex, column));
            break;
          case INT64:
            valueStatistics.update(
                time, alignedChunkData.getLongByValueIndex(originRowIndex, column));
            break;
          case DOUBLE:
            valueStatistics.update(
                time, alignedChunkData.getDoubleByValueIndex(originRowIndex, column));
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataType);
        }
      }
      valueStatistics.setEmpty(false);
    }
    IChunkMetadata vectorChunkMetadata =
        new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
    vectorChunkMetadata.setChunkLoader(new MemAlignedChunkLoader(this));
    vectorChunkMetadata.setVersion(Long.MAX_VALUE);
    cachedMetaData = vectorChunkMetadata;
  }

  @Override
  public IPointReader getPointReader() {
    chunkPointReader =
        chunkData.getAlignedIterator(floatPrecision, encodingList, chunkDataSize, deletionList);
    return chunkPointReader;
  }
}
