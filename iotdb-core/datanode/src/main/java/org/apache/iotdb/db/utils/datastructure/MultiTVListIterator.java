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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MultiTVListIterator implements MemPointIterator {
  protected TSDataType tsDataType;
  protected List<TVList.TVListIterator> tvListIterators;
  protected List<TsBlock> tsBlocks;
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
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    this.tsDataType = tsDataType;
    this.tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(deletionList, null, null, maxNumberOfPointsInPage));
    }
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
    this.encoding = encoding;
    this.tsBlocks = new ArrayList<>();
    this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
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
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    TimeValuePair currentTvPair =
        iterator.getTVList().getTimeValuePair(rowIndex, currentTime, floatPrecision, encoding);
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
    while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
      TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
      builder.getTimeColumnBuilder().writeLong(currentTime);
      switch (tsDataType) {
        case BOOLEAN:
          builder.getColumnBuilder(0).writeBoolean(iterator.getTVList().getBoolean(rowIndex));
          break;
        case INT32:
        case DATE:
          builder.getColumnBuilder(0).writeInt(iterator.getTVList().getInt(rowIndex));
          break;
        case INT64:
        case TIMESTAMP:
          builder.getColumnBuilder(0).writeLong(iterator.getTVList().getLong(rowIndex));
          break;
        case FLOAT:
          TVList floatTvList = iterator.getTVList();
          builder
              .getColumnBuilder(0)
              .writeFloat(
                  floatTvList.roundValueWithGivenPrecision(
                      floatTvList.getFloat(rowIndex), floatPrecision, encoding));
          break;
        case DOUBLE:
          TVList doubleTvList = iterator.getTVList();
          builder
              .getColumnBuilder(0)
              .writeDouble(
                  doubleTvList.roundValueWithGivenPrecision(
                      doubleTvList.getDouble(rowIndex), floatPrecision, encoding));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          builder.getColumnBuilder(0).writeBinary(iterator.getTVList().getBinary(rowIndex));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
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

  @Override
  public void close() throws IOException {
    tsBlocks.clear();
  }

  protected abstract void prepareNext();

  protected abstract void next();
}
