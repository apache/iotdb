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
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

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

  private MergeSortAlignedTVListIterator timeValuePairIterator;

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
    this.pageOffsetsList = new ArrayList<>();
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
    Statistics<? extends Serializable>[] chunkValueStatistics =
        new Statistics[valueChunkNames.size()];

    // create MergeSortAlignedTVListIterator
    List<AlignedTVList> alignedTvLists =
        alignedTvListQueryMap.keySet().stream()
            .map(x -> (AlignedTVList) x)
            .collect(Collectors.toList());

    timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists, dataTypes, columnIndexList, floatPrecision, encodingList);
    int[] alignedTvListOffsets = timeValuePairIterator.getAlignedTVListOffsets();

    int pointsInChunk = 0;
    int[] timeDeleteCursor = new int[] {0};
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    Statistics<? extends Serializable> pageTimeStats = null;
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.currentTimeValuePair();
      TsPrimitiveType[] values = tvPair.getValue().getVector();

      // ignore all-null row
      BitMap bitMap = new BitMap(dataTypes.size());
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        if (values[columnIndex] == null) {
          bitMap.mark(columnIndex);
        } else if (valueColumnsDeletionList != null
            && isPointDeleted(
                tvPair.getTimestamp(),
                valueColumnsDeletionList.get(columnIndex),
                valueColumnDeleteCursor.get(columnIndex))) {
          values[columnIndex] = null;
          bitMap.mark(columnIndex);
        }
      }
      if (bitMap.isAllMarked()) {
        timeValuePairIterator.step();
        continue;
      }

      // create pageTimeStatistics and pageValueStatistics for new page
      if (pointsInChunk % MAX_NUMBER_OF_POINTS_IN_PAGE == 0) {
        Statistics<? extends Serializable> pageTimeStatistics =
            Statistics.getStatsByType(TSDataType.VECTOR);
        pageTimeStatistics.setEmpty(false);
        timeStatisticsList.add(pageTimeStatistics);
        pageTimeStats = timeStatisticsList.get(timeStatisticsList.size() - 1);

        Statistics<? extends Serializable>[] pageValueStatistics =
            new Statistics[valueChunkNames.size()];
        Arrays.fill(pageValueStatistics, null);
        valueStatisticsList.add(pageValueStatistics);
        pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));
      }

      pageTimeStats.update(tvPair.getTimestamp());
      chunkTimeStatistics.update(tvPair.getTimestamp());

      Statistics<? extends Serializable>[] pageValuesStats =
          valueStatisticsList.get(valueStatisticsList.size() - 1);
      TsPrimitiveType[] primitiveValues = tvPair.getValue().getVector();
      for (int column = 0; column < primitiveValues.length; column++) {
        if (primitiveValues[column] == null) {
          continue;
        }

        if (pageValuesStats[column] == null) {
          Statistics<? extends Serializable> valueStatistics =
              Statistics.getStatsByType(dataTypes.get(column));
          pageValuesStats[column] = valueStatistics;
        }
        if (chunkValueStatistics[column] == null) {
          Statistics<? extends Serializable> chunkValueStats =
              Statistics.getStatsByType(dataTypes.get(column));
          chunkValueStatistics[column] = chunkValueStats;
        }

        switch (dataTypes.get(column)) {
          case BOOLEAN:
            pageValuesStats[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getBoolean());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getBoolean());
            break;
          case INT32:
          case DATE:
            pageValuesStats[column].update(tvPair.getTimestamp(), primitiveValues[column].getInt());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getInt());
            break;
          case INT64:
          case TIMESTAMP:
            pageValuesStats[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getLong());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getLong());
            break;
          case FLOAT:
            pageValuesStats[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getFloat());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getFloat());
            break;
          case DOUBLE:
            pageValuesStats[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getDouble());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            pageValuesStats[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getBinary());
            chunkValueStatistics[column].update(
                tvPair.getTimestamp(), primitiveValues[column].getBinary());
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes.get(column)));
        }
      }
      timeValuePairIterator.step();
      pointsInChunk++;
    }

    pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));
    chunkTimeStatistics.setEmpty(pointsInChunk == 0);

    // aligned chunk meta
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    for (int column = 0; column < valueChunkNames.size(); column++) {
      if (chunkValueStatistics[column] != null) {
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
    MergeSortAlignedTVListIterator timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists, dataTypes, columnIndexList, floatPrecision, encodingList);

    int[] timeDeleteCursor = new int[] {0};
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      TsPrimitiveType[] values = tvPair.getValue().getVector();

      BitMap bitMap = new BitMap(values.length);
      for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
        if (values[columnIndex] == null) {
          bitMap.mark(columnIndex);
        } else if (valueColumnsDeletionList != null
            && isPointDeleted(
                tvPair.getTimestamp(),
                valueColumnsDeletionList.get(columnIndex),
                valueColumnDeleteCursor.get(columnIndex))) {
          values[columnIndex] = null;
          bitMap.mark(columnIndex);
        }
      }
      if (bitMap.isAllMarked()) {
        continue;
      }

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

  public List<Integer> getColumnIndexList() {
    return columnIndexList;
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

  public List<Statistics<? extends Serializable>[]> getValuesStatisticsList() {
    return valueStatisticsList;
  }

  public MergeSortAlignedTVListIterator getMergeSortAlignedTVListIterator() {
    return timeValuePairIterator;
  }
}
