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

import org.apache.iotdb.commons.utils.TestOnly;
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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {

  private final String timeChunkName;

  private final List<String> valueChunkNames;

  private final List<TSDataType> dataTypes;

  private final List<List<TimeRange>> deletionList;

  private static final Logger logger = LoggerFactory.getLogger(AlignedReadOnlyMemChunk.class);

  private AlignedTVList chunkData;

  /**
   * The constructor for Aligned type.
   *
   * @param schema VectorMeasurementSchema
   * @param tvList VectorTvList
   * @param deletionList The timeRange of deletionList
   */
  public AlignedReadOnlyMemChunk(
      IMeasurementSchema schema, TVList tvList, List<List<TimeRange>> deletionList)
      throws QueryProcessException {
    super();
    this.timeChunkName = schema.getMeasurementId();
    this.valueChunkNames = schema.getSubMeasurementsList();
    this.dataTypes = schema.getSubMeasurementsTSDataTypeList();
    int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    List<TSEncoding> encodingList = schema.getSubMeasurementsTSEncodingList();
    this.chunkData = (AlignedTVList) tvList;
    this.deletionList = deletionList;
    this.tsBlock = chunkData.buildTsBlock(floatPrecision, encodingList, deletionList);
    initAlignedChunkMetaFromTsBlock();
  }

  private void initAlignedChunkMetaFromTsBlock() throws QueryProcessException {
    // time chunk
    Statistics timeStatistics = Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, 0, timeStatistics);
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    // update time chunk
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      timeStatistics.update(tsBlock.getTimeColumn().getLong(row));
    }
    timeStatistics.setEmpty(false);
    // update value chunk
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      // empty value chunk for non-exist value column
      if (tsBlock.getColumn(column) == null) {
        Statistics valueStatistics = Statistics.getStatsByType(dataTypes.get(column));
        valueStatistics.setEmpty(true);
        IChunkMetadata valueChunkMetadata =
            new ChunkMetadata(
                valueChunkNames.get(column), dataTypes.get(column), 0, valueStatistics);
        valueChunkMetadataList.add(valueChunkMetadata);
        continue;
      }
      Statistics valueStatistics =
          Statistics.getStatsByType(tsBlock.getColumn(column).getDataType());
      IChunkMetadata valueChunkMetadata =
          new ChunkMetadata(
              valueChunkNames.get(column),
              tsBlock.getColumn(column).getDataType(),
              0,
              valueStatistics);
      valueChunkMetadataList.add(valueChunkMetadata);
      for (int row = 0; row < tsBlock.getPositionCount(); row++) {
        long time = tsBlock.getTimeColumn().getLong(row);
        if (tsBlock.getColumn(column).isNull(row)) {
          continue;
        }
        switch (tsBlock.getColumn(column).getDataType()) {
          case BOOLEAN:
            valueStatistics.update(time, tsBlock.getColumn(column).getBoolean(row));
            break;
          case TEXT:
            valueStatistics.update(time, tsBlock.getColumn(column).getBinary(row));
            break;
          case FLOAT:
            valueStatistics.update(time, tsBlock.getColumn(column).getFloat(row));
            break;
          case INT32:
            valueStatistics.update(time, tsBlock.getColumn(column).getInt(row));
            break;
          case INT64:
            valueStatistics.update(time, tsBlock.getColumn(column).getLong(row));
            break;
          case DOUBLE:
            valueStatistics.update(time, tsBlock.getColumn(column).getDouble(row));
            break;
          default:
            throw new QueryProcessException(
                "Unsupported data type:" + tsBlock.getColumn(column).getDataType());
        }
      }
      valueStatistics.setEmpty(false);
    }
    IChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
    alignedChunkMetadata.setChunkLoader(new MemAlignedChunkLoader(this));
    alignedChunkMetadata.setVersion(Long.MAX_VALUE);
    cachedMetaData = alignedChunkMetadata;
  }

  @Override
  public boolean isEmpty() throws IOException {
    return tsBlock.isEmpty();
  }

  @TestOnly
  @Override
  public IPointReader getPointReader() {
    return chunkData.getAlignedIterator(null, null, deletionList);
  }
}
