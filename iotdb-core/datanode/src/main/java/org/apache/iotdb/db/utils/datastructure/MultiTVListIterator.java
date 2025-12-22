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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MultiTVListIterator extends MemPointIterator {
  protected TSDataType tsDataType;
  protected List<TVList.TVListIterator> tvListIterators;
  protected int floatPrecision;
  protected TSEncoding encoding;

  protected boolean probeNext = false;
  protected boolean hasNext = false;
  protected long currentTime = 0;
  protected int iteratorIndex = 0;
  protected int rowIndex = 0;

  // used by nextBatch during query
  protected final int maxNumberOfPointsInPage;

  protected MultiTVListIterator(
      Ordering scanOrder,
      Filter globalTimeFilter,
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<Integer> tvListRowCounts,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    super(scanOrder);
    this.tsDataType = tsDataType;
    this.tvListIterators = new ArrayList<>(tvLists.size());
    if (scanOrder.isAscending()) {
      for (int i = 0; i < tvLists.size(); i++) {
        TVList tvList = tvLists.get(i);
        int rowCount = tvListRowCounts == null ? tvList.rowCount : tvListRowCounts.get(i);
        TVList.TVListIterator iterator =
            tvList.iterator(
                scanOrder,
                rowCount,
                globalTimeFilter,
                deletionList,
                null,
                null,
                maxNumberOfPointsInPage);
        tvListIterators.add(iterator);
      }
    } else {
      for (int i = tvLists.size() - 1; i >= 0; i--) {
        TVList tvList = tvLists.get(i);
        int rowCount = tvListRowCounts == null ? tvList.rowCount : tvListRowCounts.get(i);
        TVList.TVListIterator iterator =
            tvList.iterator(
                scanOrder,
                rowCount,
                globalTimeFilter,
                deletionList,
                null,
                null,
                maxNumberOfPointsInPage);
        tvListIterators.add(iterator);
      }
    }
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
    this.encoding = encoding;
    this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
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
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    TimeValuePair currentTvPair =
        iterator
            .getTVList()
            .getTimeValuePair(
                iterator.getScanOrderIndex(rowIndex), currentTime, floatPrecision, encoding);
    next();
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    return iterator.getTVList().getTimeValuePair(rowIndex, currentTime, floatPrecision, encoding);
  }

  @Override
  public boolean hasNextBatch() {
    return hasNextTimeValuePair();
  }

  @Override
  public TsBlock nextBatch() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));
    switch (tsDataType) {
      case BOOLEAN:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          boolean aBoolean = iterator.getTVList().getBoolean(iterator.getScanOrderIndex(rowIndex));
          if (pushDownFilter == null || pushDownFilter.satisfyBoolean(currentTime, aBoolean)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeBoolean(aBoolean);
            builder.declarePosition();
          }
          next();
        }
        break;
      case INT32:
      case DATE:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          int anInt = iterator.getTVList().getInt(iterator.getScanOrderIndex(rowIndex));
          if (pushDownFilter == null || pushDownFilter.satisfyInteger(currentTime, anInt)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeInt(anInt);
            builder.declarePosition();
          }
          next();
        }
        break;
      case INT64:
      case TIMESTAMP:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          long aLong = iterator.getTVList().getLong(iterator.getScanOrderIndex(rowIndex));
          if (pushDownFilter == null || pushDownFilter.satisfyLong(currentTime, aLong)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeLong(aLong);
            builder.declarePosition();
          }
          next();
        }
        break;
      case FLOAT:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          TVList floatTvList = iterator.getTVList();
          float aFloat =
              floatTvList.roundValueWithGivenPrecision(
                  floatTvList.getFloat(iterator.getScanOrderIndex(rowIndex)),
                  floatPrecision,
                  encoding);
          if (pushDownFilter == null || pushDownFilter.satisfyFloat(currentTime, aFloat)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeFloat(aFloat);
            builder.declarePosition();
          }
          next();
        }
        break;
      case DOUBLE:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          TVList doubleTvList = iterator.getTVList();
          double aDouble =
              doubleTvList.roundValueWithGivenPrecision(
                  doubleTvList.getDouble(iterator.getScanOrderIndex(rowIndex)),
                  floatPrecision,
                  encoding);
          if (pushDownFilter == null || pushDownFilter.satisfyDouble(currentTime, aDouble)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeDouble(aDouble);
            builder.declarePosition();
          }
          next();
        }
        break;
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
          TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
          Binary binary = iterator.getTVList().getBinary(iterator.getScanOrderIndex(rowIndex));
          if (pushDownFilter == null || pushDownFilter.satisfyBinary(currentTime, binary)) {
            builder.getTimeColumnBuilder().writeLong(currentTime);
            builder.getColumnBuilder(0).writeBinary(binary);
            builder.declarePosition();
          }
          next();
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", tsDataType));
    }
    // There is no need to process pushDownFilter here because it has been applied when
    // constructing the tsBlock
    TsBlock tsBlock = paginationController.applyTsBlock(builder.build());
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

  protected abstract void prepareNext();

  protected abstract void next();

  @Override
  public void setPushDownFilter(Filter pushDownFilter) {
    for (TVList.TVListIterator iterator : tvListIterators) {
      iterator.setPushDownFilter(pushDownFilter);
    }
    this.pushDownFilter = pushDownFilter;
  }

  @Override
  public void setLimitAndOffset(PaginationController paginationController) {
    for (TVList.TVListIterator iterator : tvListIterators) {
      iterator.setLimitAndOffset(paginationController);
    }
    this.paginationController = paginationController;
  }
}
