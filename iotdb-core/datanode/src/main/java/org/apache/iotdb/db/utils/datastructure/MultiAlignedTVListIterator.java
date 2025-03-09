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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiAlignedTVListIterator implements MemPointIterator {
  protected List<TSDataType> tsDataTypeList;
  protected List<Integer> columnIndexList;
  protected List<AlignedTVList.AlignedTVListIterator> alignedTvListIterators;
  protected int floatPrecision;
  protected List<TSEncoding> encodingList;
  protected List<List<TimeRange>> valueColumnsDeletionList;

  protected boolean probeNext = false;
  protected boolean hasNext = false;

  protected List<TsBlock> tsBlocks;
  protected long currentTime;

  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected MultiAlignedTVListIterator(
      List<TSDataType> tsDataTypeList,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList) {
    this.tsDataTypeList = tsDataTypeList;
    this.columnIndexList = columnIndexList;
    this.alignedTvListIterators = new ArrayList<>(alignedTvLists.size());
    for (AlignedTVList alignedTvList : alignedTvLists) {
      alignedTvListIterators.add(
          alignedTvList.iterator(tsDataTypeList, columnIndexList, floatPrecision, encodingList));
    }
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
    this.encodingList = encodingList;
    this.tsBlocks = new ArrayList<>();
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNext();
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TsPrimitiveType[] vector = new TsPrimitiveType[tsDataTypeList.size()];
    for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
      AlignedTVList.AlignedTVListIterator iterator =
          alignedTvListIterators.get(currentIteratorIndex(columnIndex));
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
      AlignedTVList.AlignedTVListIterator iterator =
          alignedTvListIterators.get(currentIteratorIndex(columnIndex));
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
    TsBlockBuilder builder = new TsBlockBuilder(tsDataTypeList);
    // Time column
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();

    while (hasNextTimeValuePair() && builder.getPositionCount() < MAX_NUMBER_OF_POINTS_IN_PAGE) {
      timeBuilder.writeLong(currentTime);
      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        // Value column
        ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
        AlignedTVList alignedTVList =
            alignedTvListIterators.get(currentIteratorIndex(columnIndex)).getAlignedTVList();

        // sanity check
        int validColumnIndex =
            columnIndexList != null ? columnIndexList.get(columnIndex) : columnIndex;
        if (validColumnIndex < 0 || validColumnIndex >= alignedTVList.dataTypes.size()) {
          valueBuilder.appendNull();
          continue;
        }

        // null value
        if (alignedTVList.isNullValue(currentRowIndex(columnIndex), validColumnIndex)) {
          valueBuilder.appendNull();
          continue;
        }

        switch (tsDataTypeList.get(columnIndex)) {
          case BOOLEAN:
            valueBuilder.writeBoolean(
                alignedTVList.getBooleanByValueIndex(
                    currentRowIndex(columnIndex), validColumnIndex));
            break;
          case INT32:
          case DATE:
            valueBuilder.writeInt(
                alignedTVList.getIntByValueIndex(currentRowIndex(columnIndex), validColumnIndex));
            break;
          case INT64:
          case TIMESTAMP:
            valueBuilder.writeLong(
                alignedTVList.getLongByValueIndex(currentRowIndex(columnIndex), validColumnIndex));
            break;
          case FLOAT:
            float valueF =
                alignedTVList.getFloatByValueIndex(currentRowIndex(columnIndex), validColumnIndex);
            if (encodingList != null) {
              valueF =
                  alignedTVList.roundValueWithGivenPrecision(
                      valueF, floatPrecision, encodingList.get(columnIndex));
            }
            valueBuilder.writeFloat(valueF);
            break;
          case DOUBLE:
            double valueD =
                alignedTVList.getDoubleByValueIndex(currentRowIndex(columnIndex), validColumnIndex);
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
            valueBuilder.writeBinary(
                alignedTVList.getBinaryByValueIndex(
                    currentRowIndex(columnIndex), validColumnIndex));
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
    tsBlocks.add(tsBlock);
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
}
