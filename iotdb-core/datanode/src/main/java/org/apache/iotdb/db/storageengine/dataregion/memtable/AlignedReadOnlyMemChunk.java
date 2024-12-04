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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedChunkLoader;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {
  private final String timeChunkName;

  private final List<String> valueChunkNames;

  private final List<TSDataType> dataTypes;

  /**
   * The constructor for Aligned type.
   *
   * @param schema VectorMeasurementSchema
   * @param tvList VectorTvList
   * @param deletionList The timeRange of deletionList
   * @throws QueryProcessException if there is unsupported data type.
   */
  public AlignedReadOnlyMemChunk(
      QueryContext context,
      IMeasurementSchema schema,
      TVList tvList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList)
      throws QueryProcessException {
    super(context);
    this.timeChunkName = schema.getMeasurementName();
    this.valueChunkNames = schema.getSubMeasurementsList();
    this.dataTypes = schema.getSubMeasurementsTSDataTypeList();
    int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    List<TSEncoding> encodingList = schema.getSubMeasurementsTSEncodingList();
    this.tsBlock =
        ((AlignedTVList) tvList)
            .buildTsBlock(
                floatPrecision,
                encodingList,
                timeColumnDeletion,
                valueColumnsDeletionList,
                context.isIgnoreAllNullRows());
    initAlignedChunkMetaFromTsBlock();
  }

  private void initAlignedChunkMetaFromTsBlock() throws QueryProcessException {
    // Time chunk
    Statistics timeStatistics = Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, null, null, 0, timeStatistics);
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    // Update time chunk
    for (int row = 0; row < tsBlock.getPositionCount(); row++) {
      timeStatistics.update(tsBlock.getTimeColumn().getLong(row));
    }
    timeStatistics.setEmpty(false);
    // Update value chunk
    for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
      Statistics valueStatistics = Statistics.getStatsByType(dataTypes.get(column));
      valueStatistics.setEmpty(true);
      switch (dataTypes.get(column)) {
        case BOOLEAN:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getBoolean(row));
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getBinary(row));
            }
          }
          break;
        case FLOAT:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getFloat(row));
            }
          }
          break;
        case INT32:
        case DATE:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getInt(row));
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getLong(row));
            }
          }
          break;
        case DOUBLE:
          for (int row = 0; row < tsBlock.getPositionCount(); row++) {
            if (!tsBlock.getColumn(column).isNull(row)) {
              long time = tsBlock.getTimeColumn().getLong(row);
              valueStatistics.update(time, tsBlock.getColumn(column).getDouble(row));
            }
          }
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes.get(column));
      }
      if (valueStatistics.getCount() > 0) {
        IChunkMetadata valueChunkMetadata =
            new ChunkMetadata(
                valueChunkNames.get(column), dataTypes.get(column), null, null, 0, valueStatistics);
        valueChunkMetadataList.add(valueChunkMetadata);
        valueStatistics.setEmpty(false);
      } else {
        valueChunkMetadataList.add(null);
      }
    }
    IChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
    alignedChunkMetadata.setChunkLoader(new MemAlignedChunkLoader(context, this));
    alignedChunkMetadata.setVersion(Long.MAX_VALUE);
    cachedMetaData = alignedChunkMetadata;
  }

  @Override
  public boolean isEmpty() {
    return tsBlock.isEmpty();
  }

  @Override
  public IPointReader getPointReader() {
    return tsBlock.getTsBlockAlignedRowIterator();
  }
}
