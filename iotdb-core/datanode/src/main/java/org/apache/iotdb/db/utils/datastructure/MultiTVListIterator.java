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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MultiTVListIterator extends MemPointIterator {
  protected TSDataType tsDataType;
  protected List<TVList.TVListIterator> tvListIterators;
  protected int floatPrecision;
  protected TSEncoding encoding;
  protected final TypeServices.TVListBatchWriter batchWriter;

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
      int maxNumberOfPointsInPage,
      QueryContext queryContext) {
    super(scanOrder, queryContext);
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
                maxNumberOfPointsInPage,
                queryContext);
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
                maxNumberOfPointsInPage,
                queryContext);
        tvListIterators.add(iterator);
      }
    }
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
    this.encoding = encoding;
    this.batchWriter =
        TypeServices.TV_LIST_BATCH_WRITER_SERVICE.call(Type.fromTsDataType(tsDataType));
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
    long filteredRowsByPushDownFilter = 0;
    while (hasNextTimeValuePair() && builder.getPositionCount() < maxNumberOfPointsInPage) {
      TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
      TVList tvList = iterator.getTVList();
      if (!batchWriter.write(
          tvList,
          iterator.getScanOrderIndex(rowIndex),
          currentTime,
          pushDownFilter,
          builder,
          floatPrecision,
          encoding,
          null)) {
        filteredRowsByPushDownFilter++;
      }
      next();
    }

    if (this.getQueryContext().isVerbose() && filteredRowsByPushDownFilter > 0) {
      this.getQueryContext()
          .getQueryStatistics()
          .addFilteredRowsOfRowLevel(filteredRowsByPushDownFilter);
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
