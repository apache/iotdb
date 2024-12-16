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
import org.apache.iotdb.db.utils.datastructure.MergeSortAlignedTVListIterator;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {
  private final String timeChunkName;

  private final List<String> valueChunkNames;

  private final List<TSDataType> dataTypes;

  private final int floatPrecision;
  private final List<TSEncoding> encodingList;

  private final List<TimeRange> timeColumnDeletion;
  private final List<List<TimeRange>> valueColumnsDeletionList;

  // time & values statistics
  private final List<Statistics<? extends Serializable>> timeStatisticsList;
  private final List<List<Statistics<? extends Serializable>>> valueStatisticsList;

  // AlignedTVList rowCount during query
  protected Map<AlignedTVList, Integer> alignedTvListQueryMap;

  private final List<Integer> columnIndexList;

  /**
   * The constructor for Aligned type.
   *
   * @param context query context
   * @param schema VectorMeasurementSchema
   * @param alignedTvListQueryMap VectorTvList
   * @param timeColumnDeletion The timeRange of deletionList
   * @param valueColumnsDeletionList time value column deletionList
   * @throws QueryProcessException if there is unsupported data type.
   */
  public AlignedReadOnlyMemChunk(
      QueryContext context,
      List<Integer> columnIndexList,
      IMeasurementSchema schema,
      Map<AlignedTVList, Integer> alignedTvListQueryMap,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList) {
    super(context);
    this.pageOffsetsList = new ArrayList<>();
    this.timeChunkName = schema.getMeasurementName();
    this.valueChunkNames = schema.getSubMeasurementsList();
    this.dataTypes = schema.getSubMeasurementsTSDataTypeList();
    this.floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    this.encodingList = schema.getSubMeasurementsTSEncodingList();
    this.timeColumnDeletion = timeColumnDeletion;
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.timeStatisticsList = new ArrayList<>();
    this.valueStatisticsList = new ArrayList<>();
    this.alignedTvListQueryMap = alignedTvListQueryMap;
    this.columnIndexList = columnIndexList;
    this.context.addAlignedTVListToSet(alignedTvListQueryMap);
  }

  @Override
  public void sortTvLists() {
    for (Map.Entry<AlignedTVList, Integer> entry : getAligendTvListQueryMap().entrySet()) {
      AlignedTVList alignedTvList = entry.getKey();
      int queryRowCount = entry.getValue();
      if (!alignedTvList.isSorted() && queryRowCount > alignedTvList.seqRowCount()) {
        alignedTvList.safelySort();
      }
    }
  }

  @Override
  public void initChunkMetaFromTvLists() {
    // init chunk meta
    Statistics<? extends Serializable> chunkTimeStatistics =
        Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata chunkTimeMetadata =
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, null, null, 0, chunkTimeStatistics);
    List<Statistics<? extends Serializable>> chunkValueStatistics = new ArrayList<>();
    List<IChunkMetadata> chunkValueMetadataList = new ArrayList<>();
    for (int column = 0; column < valueChunkNames.size(); column++) {
      Statistics<? extends Serializable> valueStatistics =
          Statistics.getStatsByType(dataTypes.get(column));
      chunkValueStatistics.add(valueStatistics);
      IChunkMetadata valueChunkMetadata =
          new ChunkMetadata(
              valueChunkNames.get(column), dataTypes.get(column), null, null, 0, valueStatistics);
      chunkValueMetadataList.add(valueChunkMetadata);
    }

    int cnt = 0;
    List<AlignedTVList> alignedTvLists = new ArrayList<>(alignedTvListQueryMap.keySet());
    MergeSortAlignedTVListIterator timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists,
            columnIndexList,
            floatPrecision,
            encodingList,
            timeColumnDeletion,
            valueColumnsDeletionList,
            context.isIgnoreAllNullRows());
    int[] alignedTvListOffsets = timeValuePairIterator.getAlignedTVListOffsets();
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      // Split pages
      if (cnt % MAX_NUMBER_OF_POINTS_IN_PAGE == 0) {
        Statistics<? extends Serializable> pageTimeStatistics =
            Statistics.getStatsByType(TSDataType.VECTOR);
        pageTimeStatistics.setEmpty(false);
        timeStatisticsList.add(pageTimeStatistics);
        List<Statistics<? extends Serializable>> pageValueStatistics = new ArrayList<>();
        for (int column = 0; column < valueChunkNames.size(); column++) {
          Statistics<? extends Serializable> valueStatistics =
              Statistics.getStatsByType(dataTypes.get(column));
          pageValueStatistics.add(valueStatistics);
        }
        valueStatisticsList.add(pageValueStatistics);
        pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));
      }

      // Update Page & Chunk Statistics
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      Statistics<? extends Serializable> pageTimeStats =
          timeStatisticsList.get(timeStatisticsList.size() - 1);
      pageTimeStats.update(tvPair.getTimestamp());
      chunkTimeStatistics.update(tvPair.getTimestamp());

      List<Statistics<? extends Serializable>> pageValuesStats =
          valueStatisticsList.get(valueStatisticsList.size() - 1);
      TsPrimitiveType[] primitiveValues = tvPair.getValue().getVector();
      for (int column = 0; column < primitiveValues.length; column++) {
        if (primitiveValues[column] == null) {
          continue;
        }
        switch (dataTypes.get(column)) {
          case BOOLEAN:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getBoolean());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getBoolean());
            break;
          case INT32:
          case DATE:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getInt());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getInt());
            break;
          case INT64:
          case TIMESTAMP:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getLong());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getLong());
            break;
          case FLOAT:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getFloat());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getFloat());
            break;
          case DOUBLE:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getDouble());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            pageValuesStats
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getBinary());
            chunkValueStatistics
                .get(column)
                .update(tvPair.getTimestamp(), primitiveValues[column].getBinary());
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes.get(column)));
        }
      }
      cnt++;
    }
    pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));
    chunkTimeStatistics.setEmpty(cnt == 0);

    // statistics should be set null if there is no data
    for (int column = 0; column < chunkValueMetadataList.size(); column++) {
      if (chunkValueMetadataList.get(column).getStatistics().isEmpty()) {
        chunkValueMetadataList.set(column, null);
      }
    }
    for (List<Statistics<? extends Serializable>> pageValueStats : valueStatisticsList) {
      for (int column = 0; column < pageValueStats.size(); column++) {
        if (pageValueStats.get(column).isEmpty()) {
          pageValueStats.set(column, null);
        }
      }
    }

    // aligned chunk meta
    IChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(chunkTimeMetadata, chunkValueMetadataList);
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
    for (Map.Entry<AlignedTVList, Integer> entry : alignedTvListQueryMap.entrySet()) {
      AlignedTVList tvList = entry.getKey();
      int queryLength = entry.getValue();
      if (!tvList.isSorted() && queryLength > tvList.seqRowCount()) {
        tvList.safelySort();
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
    List<AlignedTVList> alignedTvLists = new ArrayList<>(alignedTvListQueryMap.keySet());
    IPointReader timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists,
            columnIndexList,
            floatPrecision,
            encodingList,
            timeColumnDeletion,
            valueColumnsDeletionList,
            context.isIgnoreAllNullRows());
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      builder.getTimeColumnBuilder().writeLong(tvPair.getTimestamp());

      // value columns
      TsPrimitiveType[] values = tvPair.getValue().getVector();
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

  public Map<AlignedTVList, Integer> getAligendTvListQueryMap() {
    return alignedTvListQueryMap;
  }

  @Override
  public int getFloatPrecision() {
    return floatPrecision;
  }

  public List<Integer> getColumnIndexList() {
    return columnIndexList;
  }

  public List<TimeRange> getTimeColumnDeletion() {
    return timeColumnDeletion;
  }

  public List<List<TimeRange>> getValueColumnsDeletionList() {
    return valueColumnsDeletionList;
  }

  public List<TSEncoding> getEncodingList() {
    return encodingList;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<Statistics<? extends Serializable>> getTimeStatisticsList() {
    return timeStatisticsList;
  }

  public List<List<Statistics<? extends Serializable>>> getValuesStatisticsList() {
    return valueStatisticsList;
  }
}
