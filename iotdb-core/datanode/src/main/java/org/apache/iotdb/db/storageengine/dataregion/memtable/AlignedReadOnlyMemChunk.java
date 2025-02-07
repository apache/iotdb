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
import org.apache.iotdb.db.utils.datastructure.PageColumnAccessInfo;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.TableDeviceChunkMetadata;
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

  private final List<TimeRange> timeColumnDeletion;
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
   * @param timeColumnDeletion The timeRange of deletionList
   * @param valueColumnsDeletionList time value column deletionList
   */
  public AlignedReadOnlyMemChunk(
      QueryContext context,
      List<Integer> columnIndexList,
      IMeasurementSchema schema,
      Map<TVList, Integer> alignedTvListQueryMap,
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
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, null, null, 0, chunkTimeStatistics);
    Statistics<? extends Serializable>[] chunkValueStatistics =
        new Statistics[valueChunkNames.size()];

    // create MergeSortAlignedTVListIterator
    List<AlignedTVList> alignedTvLists =
        alignedTvListQueryMap.keySet().stream()
            .map(x -> (AlignedTVList) x)
            .collect(Collectors.toList());

    timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists,
            dataTypes,
            columnIndexList,
            floatPrecision,
            encodingList,
            context.isIgnoreAllNullRows());
    int[] alignedTvListOffsets = timeValuePairIterator.getAlignedTVListOffsets();

    // iterate to build column access info and split pages
    int pointsInPage = 0;
    long[] time = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];
    PageColumnAccessInfo[] pageColumnAccessInfo = new PageColumnAccessInfo[dataTypes.size()];
    for (int i = 0; i < pageColumnAccessInfo.length; i++) {
      pageColumnAccessInfo[i] = new PageColumnAccessInfo(MAX_NUMBER_OF_POINTS_IN_PAGE);
    }

    int[] timeDeleteCursor = new int[] {0};
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      long timestamp = timeValuePairIterator.getTime();
      // ignore deleted row
      if (timeColumnDeletion != null
          && isPointDeleted(timestamp, timeColumnDeletion, timeDeleteCursor)) {
        timeValuePairIterator.step();
        continue;
      }

      // ignore all-null row
      BitMap bitMap = timeValuePairIterator.getBitmap();
      if (valueColumnsDeletionList != null) {
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          if (isPointDeleted(
              timestamp,
              valueColumnsDeletionList.get(columnIndex),
              valueColumnDeleteCursor.get(columnIndex))) {
            bitMap.mark(columnIndex);
          }
        }
      }
      if (context.isIgnoreAllNullRows() && bitMap.isAllMarked()) {
        timeValuePairIterator.step();
        continue;
      }

      // create pageTimeStatistics and pageValueStatistics for new page
      if (pointsInPage == 0) {
        Statistics<? extends Serializable> pageTimeStatistics =
            Statistics.getStatsByType(TSDataType.VECTOR);
        timeStatisticsList.add(pageTimeStatistics);
        Statistics<? extends Serializable>[] pageValueStatistics =
            new Statistics[valueChunkNames.size()];
        valueStatisticsList.add(pageValueStatistics);

        pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));
      }

      // prepare column access info for current page
      int[][] columnAccessInfo = timeValuePairIterator.getColumnAccessInfo();
      time[pointsInPage] = timeValuePairIterator.getTime();
      for (int i = 0; i < dataTypes.size(); i++) {
        pageColumnAccessInfo[i].add(columnAccessInfo[i]);
      }
      timeValuePairIterator.step();
      pointsInPage++;

      if (pointsInPage == MAX_NUMBER_OF_POINTS_IN_PAGE) {
        Statistics<? extends Serializable> pageTimeStatistics =
            timeStatisticsList.get(timeStatisticsList.size() - 1);
        Statistics<? extends Serializable>[] pageValueStatistics =
            valueStatisticsList.get(valueStatisticsList.size() - 1);

        // update page time & value statistics
        updateTimeStatistics(
            time, MAX_NUMBER_OF_POINTS_IN_PAGE, chunkTimeStatistics, pageTimeStatistics);
        updateValueStatistics(
            time,
            pageColumnAccessInfo,
            chunkValueStatistics,
            pageValueStatistics,
            timeValuePairIterator);

        // reset
        for (PageColumnAccessInfo p : pageColumnAccessInfo) {
          p.reset();
        }
        chunkTimeStatistics.setEmpty(false);
        pointsInPage = 0;
      }
    }

    if (pointsInPage > 0) {
      Statistics<? extends Serializable> pageTimeStatistics =
          timeStatisticsList.get(timeStatisticsList.size() - 1);
      Statistics<? extends Serializable>[] pageValueStatistics =
          valueStatisticsList.get(valueStatisticsList.size() - 1);

      updateTimeStatistics(time, pointsInPage, chunkTimeStatistics, pageTimeStatistics);
      updateValueStatistics(
          time,
          pageColumnAccessInfo,
          chunkValueStatistics,
          pageValueStatistics,
          timeValuePairIterator);
      chunkTimeStatistics.setEmpty(false);
    }
    pageOffsetsList.add(Arrays.copyOf(alignedTvListOffsets, alignedTvListOffsets.length));

    // aligned chunk meta
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    for (int column = 0; column < valueChunkNames.size(); column++) {
      if (chunkValueStatistics[column] != null) {
        IChunkMetadata valueChunkMetadata =
            new ChunkMetadata(
                valueChunkNames.get(column),
                dataTypes.get(column),
                null,
                null,
                0,
                chunkValueStatistics[column]);
        valueChunkMetadataList.add(valueChunkMetadata);
      } else {
        valueChunkMetadataList.add(null);
      }
    }

    IChunkMetadata alignedChunkMetadata =
        context.isIgnoreAllNullRows()
            ? new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList)
            : new TableDeviceChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
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

  private boolean isAllColumnNull(TsPrimitiveType[] primitiveValues) {
    for (TsPrimitiveType primitiveValue : primitiveValues) {
      if (primitiveValue != null) {
        return false;
      }
    }
    return true;
  }

  private void writeValidValuesIntoTsBlock(TsBlockBuilder builder) throws IOException {
    List<AlignedTVList> alignedTvLists =
        alignedTvListQueryMap.keySet().stream()
            .map(x -> (AlignedTVList) x)
            .collect(Collectors.toList());
    MergeSortAlignedTVListIterator timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists,
            dataTypes,
            columnIndexList,
            floatPrecision,
            encodingList,
            context.isIgnoreAllNullRows());

    int[] timeDeleteCursor = new int[] {0};
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      // skip deleted rows
      if (timeColumnDeletion != null
          && isPointDeleted(tvPair.getTimestamp(), timeColumnDeletion, timeDeleteCursor)) {
        timeValuePairIterator.step();
        continue;
      }

      TsPrimitiveType[] primitiveValues = tvPair.getValue().getVector();
      if (valueColumnsDeletionList != null) {
        for (int columnIndex = 0; columnIndex < primitiveValues.length; columnIndex++) {
          if (isPointDeleted(
              tvPair.getTimestamp(),
              valueColumnsDeletionList.get(columnIndex),
              valueColumnDeleteCursor.get(columnIndex))) {
            primitiveValues[columnIndex] = null;
          }
        }
      }
      if (context.isIgnoreAllNullRows() && isAllColumnNull(primitiveValues)) {
        timeValuePairIterator.step();
        continue;
      }

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

  public List<Statistics<? extends Serializable>[]> getValuesStatisticsList() {
    return valueStatisticsList;
  }

  public MergeSortAlignedTVListIterator getMergeSortAlignedTVListIterator() {
    return timeValuePairIterator;
  }

  private void updateTimeStatistics(
      long[] time,
      int count,
      Statistics<? extends Serializable> chunkTimeStatistics,
      Statistics<? extends Serializable> pageTimeStatistics) {
    for (int index = 0; index < count; index++) {
      chunkTimeStatistics.update(time[index]);
      pageTimeStatistics.update(time[index]);
    }
    pageTimeStatistics.setEmpty(count == 0);
  }

  private void createValueStatisticsIfNotExists(
      Statistics<? extends Serializable>[] chunkValueStatistics,
      Statistics<? extends Serializable>[] pageValueStatistics,
      List<TSDataType> dataTypes,
      int columnIndex) {
    if (pageValueStatistics[columnIndex] == null) {
      Statistics<? extends Serializable> valueStatistics =
          Statistics.getStatsByType(dataTypes.get(columnIndex));
      pageValueStatistics[columnIndex] = valueStatistics;
    }
    if (chunkValueStatistics[columnIndex] == null) {
      Statistics<? extends Serializable> chunkValueStats =
          Statistics.getStatsByType(dataTypes.get(columnIndex));
      chunkValueStatistics[columnIndex] = chunkValueStats;
    }
  }

  private void updateValueStatistics(
      long[] time,
      PageColumnAccessInfo[] columnAccessInfo,
      Statistics<? extends Serializable>[] chunkValueStatistics,
      Statistics<? extends Serializable>[] pageValueStatistics,
      MergeSortAlignedTVListIterator timeValuePairIterator) {
    // update value statistics
    for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
      PageColumnAccessInfo pageAccessInfo = columnAccessInfo[columnIndex];
      switch (dataTypes.get(columnIndex)) {
        case BOOLEAN:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getBoolean());
              chunkValueStatistics[columnIndex].update(time[index], value.getBoolean());
            }
          }
          break;
        case INT32:
        case DATE:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getInt());
              chunkValueStatistics[columnIndex].update(time[index], value.getInt());
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getLong());
              chunkValueStatistics[columnIndex].update(time[index], value.getLong());
            }
          }
          break;
        case FLOAT:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getFloat());
              chunkValueStatistics[columnIndex].update(time[index], value.getFloat());
            }
          }
          break;
        case DOUBLE:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getDouble());
              chunkValueStatistics[columnIndex].update(time[index], value.getDouble());
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          for (int index = 0; index < pageAccessInfo.count(); index++) {
            int[] accessInfo = pageAccessInfo.get(index);
            TsPrimitiveType value =
                timeValuePairIterator.getPrimitiveObject(accessInfo, columnIndex);
            if (value != null) {
              createValueStatisticsIfNotExists(
                  pageValueStatistics, chunkValueStatistics, dataTypes, columnIndex);
              pageValueStatistics[columnIndex].update(time[index], value.getBinary());
              chunkValueStatistics[columnIndex].update(time[index], value.getBinary());
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypes.get(columnIndex)));
      }
    }
  }
}
