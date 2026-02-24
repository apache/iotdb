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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.TsBlockUtil;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiAlignedTVListIterator extends MemPointIterator {
  protected List<TSDataType> tsDataTypeList;
  protected List<Integer> columnIndexList;
  protected List<AlignedTVList.AlignedTVListIterator> alignedTvListIterators;
  protected int floatPrecision;
  protected List<TSEncoding> encodingList;
  protected List<List<TimeRange>> valueColumnsDeletionList;
  protected boolean ignoreAllNullRows;

  protected boolean probeNext = false;
  protected boolean hasNext = false;

  protected long currentTime;

  // used by nextBatch during query
  protected final int maxNumberOfPointsInPage;
  protected final List<int[]> valueColumnDeleteCursor;

  protected MultiAlignedTVListIterator(
      List<TSDataType> tsDataTypeList,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    super(scanOrder);
    this.tsDataTypeList = tsDataTypeList;
    this.columnIndexList = columnIndexList;
    this.alignedTvListIterators = new ArrayList<>(alignedTvLists.size());
    if (scanOrder.isAscending()) {
      for (int i = 0; i < alignedTvLists.size(); i++) {
        AlignedTVList alignedTVList = alignedTvLists.get(i);
        AlignedTVList.AlignedTVListIterator iterator =
            alignedTVList.iterator(
                scanOrder,
                tvListRowCounts == null ? alignedTVList.rowCount() : tvListRowCounts.get(i),
                globalTimeFilter,
                tsDataTypeList,
                columnIndexList,
                timeColumnDeletion,
                null,
                floatPrecision,
                encodingList,
                ignoreAllNullRows,
                maxNumberOfPointsInPage);
        alignedTvListIterators.add(iterator);
      }
    } else {
      for (int i = alignedTvLists.size() - 1; i >= 0; i--) {
        AlignedTVList alignedTVList = alignedTvLists.get(i);
        AlignedTVList.AlignedTVListIterator iterator =
            alignedTVList.iterator(
                scanOrder,
                tvListRowCounts == null ? alignedTVList.rowCount() : tvListRowCounts.get(i),
                globalTimeFilter,
                tsDataTypeList,
                columnIndexList,
                timeColumnDeletion,
                null,
                floatPrecision,
                encodingList,
                ignoreAllNullRows,
                maxNumberOfPointsInPage);
        alignedTvListIterators.add(iterator);
      }
    }
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
    this.encodingList = encodingList;
    this.ignoreAllNullRows = ignoreAllNullRows;
    this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
    this.valueColumnDeleteCursor = new ArrayList<>();
    for (int i = 0; i < tsDataTypeList.size(); i++) {
      List<TimeRange> valueColumnDeletions =
          valueColumnsDeletionList == null ? null : valueColumnsDeletionList.get(i);
      int cursor =
          (valueColumnDeletions == null || scanOrder.isAscending())
              ? 0
              : (valueColumnDeletions.size() - 1);
      valueColumnDeleteCursor.add(new int[] {cursor});
    }
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!paginationController.hasCurLimit()) {
      return false;
    }
    if (!probeNext) {
      prepareNext();
    }
    return hasNext && !isCurrentTimeExceedTimeRange(currentTime);
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TsPrimitiveType[] vector = new TsPrimitiveType[tsDataTypeList.size()];
    for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
      int iteratorIndex = currentIteratorIndex(columnIndex);
      if (iteratorIndex == -1) {
        continue;
      }
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
      vector[columnIndex] =
          iterator.getPrimitiveTypeObject(currentRowIndex(columnIndex), columnIndex);
    }
    TimeValuePair currentTvPair =
        new TimeValuePair(currentTime, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
    next();
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TsPrimitiveType[] vector = new TsPrimitiveType[tsDataTypeList.size()];
    for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
      int iteratorIndex = currentIteratorIndex(columnIndex);
      if (iteratorIndex == -1) {
        continue;
      }
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
      vector[columnIndex] =
          iterator.getPrimitiveTypeObject(currentRowIndex(columnIndex), columnIndex);
    }
    return new TimeValuePair(currentTime, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
  }

  @Override
  public boolean hasNextBatch() {
    return hasNextTimeValuePair();
  }

  @Override
  public TsBlock nextBatch() {
    TsBlockBuilder builder = new TsBlockBuilder(maxNumberOfPointsInPage, tsDataTypeList);
    // Time column
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();

    while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
      timeBuilder.writeLong(currentTime);
      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        // Value column
        ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
        int currentIteratorIndex = currentIteratorIndex(columnIndex);
        if (currentIteratorIndex == -1) {
          valueBuilder.appendNull();
          continue;
        }
        AlignedTVList.AlignedTVListIterator alignedTVListIterator =
            alignedTvListIterators.get(currentIteratorIndex);
        AlignedTVList alignedTVList = alignedTVListIterator.getAlignedTVList();

        // sanity check
        int validColumnIndex =
            columnIndexList != null ? columnIndexList.get(columnIndex) : columnIndex;
        if (validColumnIndex < 0 || validColumnIndex >= alignedTVList.dataTypes.size()) {
          valueBuilder.appendNull();
          continue;
        }

        int valueIndex =
            alignedTVList.getValueIndex(
                alignedTVListIterator.getScanOrderIndex(currentRowIndex(columnIndex)));
        // null value
        if (alignedTVList.isNullValue(valueIndex, validColumnIndex)) {
          valueBuilder.appendNull();
          continue;
        }

        switch (tsDataTypeList.get(columnIndex)) {
          case BOOLEAN:
            valueBuilder.writeBoolean(
                alignedTVList.getBooleanByValueIndex(valueIndex, validColumnIndex));
            break;
          case INT32:
          case DATE:
            valueBuilder.writeInt(alignedTVList.getIntByValueIndex(valueIndex, validColumnIndex));
            break;
          case INT64:
          case TIMESTAMP:
            valueBuilder.writeLong(alignedTVList.getLongByValueIndex(valueIndex, validColumnIndex));
            break;
          case FLOAT:
            float valueF = alignedTVList.getFloatByValueIndex(valueIndex, validColumnIndex);
            if (encodingList != null) {
              valueF =
                  alignedTVList.roundValueWithGivenPrecision(
                      valueF, floatPrecision, encodingList.get(columnIndex));
            }
            valueBuilder.writeFloat(valueF);
            break;
          case DOUBLE:
            double valueD = alignedTVList.getDoubleByValueIndex(valueIndex, validColumnIndex);
            if (encodingList != null) {
              valueD =
                  alignedTVList.roundValueWithGivenPrecision(
                      valueD, floatPrecision, encodingList.get(columnIndex));
            }
            valueBuilder.writeDouble(valueD);
            break;
          case TEXT:
          case BLOB:
          case STRING:
          case OBJECT:
            valueBuilder.writeBinary(
                alignedTVList.getBinaryByValueIndex(valueIndex, validColumnIndex));
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", tsDataTypeList.get(columnIndex)));
        }
      }
      next();

      builder.declarePosition();
    }
    TsBlock tsBlock = builder.build();
    if (pushDownFilter != null) {
      tsBlock =
          TsBlockUtil.applyFilterAndLimitOffsetToTsBlock(
              tsBlock,
              new TsBlockBuilder(
                  Math.min(maxNumberOfPointsInPage, tsBlock.getPositionCount()), tsDataTypeList),
              pushDownFilter,
              paginationController);
    } else {
      tsBlock = paginationController.applyTsBlock(tsBlock);
    }
    addTsBlock(tsBlock);
    return tsBlock;
  }

  @Override
  public TsBlock getBatch(int tsBlockIndex) {
    if (tsBlockIndex < 0 || tsBlockIndex >= tsBlocks.size()) {
      return null;
    }
    return tsBlocks.get(tsBlockIndex);
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  protected abstract int currentIteratorIndex(int columnIndex);

  protected abstract int currentRowIndex(int columnIndex);

  @Override
  public void close() throws IOException {}

  protected abstract void prepareNext();

  protected abstract void next();

  @Override
  public void setPushDownFilter(Filter pushDownFilter) {
    for (AlignedTVList.AlignedTVListIterator iterator : alignedTvListIterators) {
      iterator.setPushDownFilter(pushDownFilter);
    }
    this.pushDownFilter = pushDownFilter;
  }

  @Override
  public void setLimitAndOffset(PaginationController paginationController) {
    for (AlignedTVList.AlignedTVListIterator iterator : alignedTvListIterators) {
      iterator.setLimitAndOffset(
          new PaginationController(
              paginationController.getCurLimit() + paginationController.getCurOffset(), 0));
    }
    this.paginationController = paginationController;
  }
}
