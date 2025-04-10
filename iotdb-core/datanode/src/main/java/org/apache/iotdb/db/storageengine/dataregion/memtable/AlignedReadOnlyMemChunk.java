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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedChunkLoader;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {
  private final String timeChunkName;

  private final List<String> valueChunkNames;

  private final List<TSDataType> dataTypes;

  private final int floatPrecision;
  private final List<TSEncoding> encodingList;

  private final List<List<TimeRange>> valueColumnsDeletionList;

  // time & values statistics
  private final List<Statistics<? extends Serializable>> timeStatisticsList;
  private final List<Statistics<? extends Serializable>[]> valueStatisticsList;

  // AlignedTVList rowCount during query
  protected Map<TVList, Integer> alignedTvListQueryMap;

  // For example, it stores time series [s1, s2, s3] in AlignedWritableMemChunk.
  // When we select two of time series [s1, s3], the column index list should be [0, 2]
  private final List<Integer> columnIndexList;

  private MemPointIterator timeValuePairIterator;

  /**
   * The constructor for Aligned type.
   *
   * @param context query context
   * @param columnIndexList column index list
   * @param schema VectorMeasurementSchema
   * @param alignedTvListQueryMap AlignedTvList map
   * @param valueColumnsDeletionList time value column deletionList
   */
  public AlignedReadOnlyMemChunk(
      QueryContext context,
      List<Integer> columnIndexList,
      IMeasurementSchema schema,
      Map<TVList, Integer> alignedTvListQueryMap,
      List<List<TimeRange>> valueColumnsDeletionList) {
    super(context);
    this.timeChunkName = schema.getMeasurementId();
    this.valueChunkNames = schema.getSubMeasurementsList();
    this.dataTypes = schema.getSubMeasurementsTSDataTypeList();
    this.floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    this.encodingList = schema.getSubMeasurementsTSEncodingList();
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.timeStatisticsList = new ArrayList<>();
    this.valueStatisticsList = new ArrayList<>();
    this.alignedTvListQueryMap = alignedTvListQueryMap;
    this.columnIndexList = columnIndexList;
    this.context.addTVListToSet(alignedTvListQueryMap);
  }

  @Override
  public void sortTvLists() {
    for (Map.Entry<TVList, Integer> entry : getAligendTvListQueryMap().entrySet()) {
      AlignedTVList alignedTvList = (AlignedTVList) entry.getKey();
      int queryRowCount = entry.getValue();
      if (!alignedTvList.isSorted() && queryRowCount > alignedTvList.seqRowCount()) {
        alignedTvList.sort();
      }
    }
  }

  @Override
  public void initChunkMetaFromTvLists() {
    // init chunk meta
    Statistics<? extends Serializable> chunkTimeStatistics =
        Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, 0, chunkTimeStatistics);
    Statistics<? extends Serializable>[] chunkValueStatistics = new Statistics[dataTypes.size()];
    for (int column = 0; column < dataTypes.size(); column++) {
      chunkValueStatistics[column] = Statistics.getStatsByType(dataTypes.get(column));
    }

    // create MergeSortAlignedTVListIterator
    List<AlignedTVList> alignedTvLists =
        alignedTvListQueryMap.keySet().stream()
            .map(x -> (AlignedTVList) x)
            .collect(Collectors.toList());

    timeValuePairIterator =
        MemPointIteratorFactory.create(
            dataTypes,
            columnIndexList,
            alignedTvLists,
            valueColumnsDeletionList,
            floatPrecision,
            encodingList,
            MAX_NUMBER_OF_POINTS_IN_PAGE);

    while (timeValuePairIterator.hasNextBatch()) {
      // create pageTimeStatistics and pageValueStatistics for new page
      Statistics<? extends Serializable> pageTimeStatistics =
          Statistics.getStatsByType(TSDataType.VECTOR);
      pageTimeStatistics.setEmpty(false);
      timeStatisticsList.add(pageTimeStatistics);

      Statistics<? extends Serializable>[] pageValueStatistics = new Statistics[dataTypes.size()];
      for (int column = 0; column < dataTypes.size(); column++) {
        pageValueStatistics[column] = Statistics.getStatsByType(dataTypes.get(column));
      }
      valueStatisticsList.add(pageValueStatistics);

      TsBlock tsBlock = timeValuePairIterator.nextBatch();
      // time column
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        pageTimeStatistics.update(tsBlock.getTimeByIndex(i));
        chunkTimeStatistics.update(tsBlock.getTimeByIndex(i));
      }
      // value columns
      for (int column = 0; column < tsBlock.getValueColumnCount(); column++) {
        Statistics<? extends Serializable> pageValueStats =
            Statistics.getStatsByType(dataTypes.get(column));
        switch (dataTypes.get(column)) {
          case BOOLEAN:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBoolean(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBoolean(i));
            }
            break;
          case INT32:
          case DATE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getInt(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getInt(i));
            }
            break;
          case INT64:
          case TIMESTAMP:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getLong(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getLong(i));
            }
            break;
          case FLOAT:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getFloat(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getFloat(i));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getDouble(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getDouble(i));
            }
            break;
          case TEXT:
          case BLOB:
          case STRING:
            for (int i = 0; i < tsBlock.getPositionCount(); i++) {
              if (tsBlock.getColumn(column).isNull(i)) {
                continue;
              }
              pageValueStats.update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBinary(i));
              chunkValueStatistics[column].update(
                  tsBlock.getTimeByIndex(i), tsBlock.getColumn(column).getBinary(i));
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes.get(column)));
        }
        pageValueStatistics[column] = pageValueStats.isEmpty() ? null : pageValueStats;
      }
    }

    // aligned chunk meta
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    for (int column = 0; column < dataTypes.size(); column++) {
      if (!chunkValueStatistics[column].isEmpty()) {
        IChunkMetadata valueChunkMetadata =
            new ChunkMetadata(
                valueChunkNames.get(column),
                dataTypes.get(column),
                0,
                chunkValueStatistics[column]);
        valueChunkMetadataList.add(valueChunkMetadata);
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

  private int count() {
    int count = 0;
    for (TVList list : alignedTvListQueryMap.keySet()) {
      count += list.count();
    }
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count() == 0;
  }

  @Override
  public IPointReader getPointReader() {
    for (Map.Entry<TVList, Integer> entry : alignedTvListQueryMap.entrySet()) {
      AlignedTVList tvList = (AlignedTVList) entry.getKey();
      int queryLength = entry.getValue();
      if (!tvList.isSorted() && queryLength > tvList.seqRowCount()) {
        tvList.sort();
      }
    }
    TsBlock tsBlock = buildTsBlock();
    return tsBlock.getTsBlockAlignedRowIterator();
  }

  private TsBlock buildTsBlock() {
    try {
      TsBlockBuilder builder = new TsBlockBuilder(dataTypes);
      writeValidValuesIntoTsBlock(builder);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeValidValuesIntoTsBlock(TsBlockBuilder builder) throws IOException {
    List<AlignedTVList> alignedTvLists =
        alignedTvListQueryMap.keySet().stream()
            .map(x -> (AlignedTVList) x)
            .collect(Collectors.toList());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            dataTypes,
            columnIndexList,
            alignedTvLists,
            valueColumnsDeletionList,
            floatPrecision,
            encodingList,
            MAX_NUMBER_OF_POINTS_IN_PAGE);

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      TsPrimitiveType[] values = tvPair.getValue().getVector();

      // time column
      builder.getTimeColumnBuilder().writeLong(tvPair.getTimestamp());
      // value columns
      for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
        if (values[columnIndex] == null) {
          builder.getColumnBuilder(columnIndex).appendNull();
          continue;
        }
        ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
        switch (dataTypes.get(columnIndex)) {
          case BOOLEAN:
            valueBuilder.writeBoolean(values[columnIndex].getBoolean());
            break;
          case INT32:
          case DATE:
            valueBuilder.writeInt(values[columnIndex].getInt());
            break;
          case INT64:
          case TIMESTAMP:
            valueBuilder.writeLong(values[columnIndex].getLong());
            break;
          case FLOAT:
            valueBuilder.writeFloat(values[columnIndex].getFloat());
            break;
          case DOUBLE:
            valueBuilder.writeDouble(values[columnIndex].getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            valueBuilder.writeBinary(values[columnIndex].getBinary());
            break;
          default:
            break;
        }
      }
      builder.declarePosition();
    }
  }

  public Map<TVList, Integer> getAligendTvListQueryMap() {
    return alignedTvListQueryMap;
  }

  @Override
  public int getFloatPrecision() {
    return floatPrecision;
  }

  public List<List<TimeRange>> getValueColumnsDeletionList() {
    return valueColumnsDeletionList;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<Statistics<? extends Serializable>> getTimeStatisticsList() {
    return timeStatisticsList;
  }

  public List<Statistics<? extends Serializable>[]> getValuesStatisticsList() {
    return valueStatisticsList;
  }

  public MemPointIterator getMemPointIterator() {
    return timeValuePairIterator;
  }
}
