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
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.List;

public class MemPointIteratorFactory {
  private MemPointIteratorFactory() {
    // forbidden construction
  }

  // TVListIterator
  private static MemPointIterator single(List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return tvLists
        .get(0)
        .iterator(
            Ordering.ASC, tvLists.get(0).rowCount, null, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TVList> tvLists, List<TimeRange> deletionList, int maxNumberOfPointsInPage) {
    return tvLists
        .get(0)
        .iterator(
            Ordering.ASC,
            tvLists.get(0).rowCount,
            null,
            deletionList,
            null,
            null,
            maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TVList> tvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return tvLists
        .get(0)
        .iterator(
            scanOrder,
            tvListRowCounts.get(0),
            globalTimeFilter,
            deletionList,
            floatPrecision,
            encoding,
            maxNumberOfPointsInPage);
  }

  // MergeSortMultiTVListIterator
  private static MemPointIterator mergeSort(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        Ordering.ASC, null, tsDataType, tvLists, null, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        Ordering.ASC,
        null,
        tsDataType,
        tvLists,
        null,
        deletionList,
        null,
        null,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        scanOrder,
        globalTimeFilter,
        tsDataType,
        tvLists,
        tvListRowCounts,
        deletionList,
        floatPrecision,
        encoding,
        maxNumberOfPointsInPage);
  }

  // OrderedMultiTVListIterator
  private static MemPointIterator ordered(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        Ordering.ASC, null, tsDataType, tvLists, null, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        Ordering.ASC,
        null,
        tsDataType,
        tvLists,
        null,
        deletionList,
        null,
        null,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        scanOrder,
        globalTimeFilter,
        tsDataType,
        tvLists,
        tvListRowCounts,
        deletionList,
        floatPrecision,
        encoding,
        maxNumberOfPointsInPage);
  }

  // AlignedTVListIterator
  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return alignedTvLists
        .get(0)
        .iterator(
            Ordering.ASC,
            alignedTvLists.get(0).rowCount,
            null,
            tsDataTypes,
            columnIndexList,
            null,
            null,
            null,
            null,
            ignoreAllNullRows,
            maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return alignedTvLists
        .get(0)
        .iterator(
            Ordering.ASC,
            alignedTvLists.get(0).rowCount,
            null,
            tsDataTypes,
            columnIndexList,
            timeColumnDeletion,
            valueColumnsDeletionList,
            null,
            null,
            ignoreAllNullRows,
            maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
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
    return alignedTvLists
        .get(0)
        .iterator(
            scanOrder,
            tvListRowCounts.get(0),
            globalTimeFilter,
            tsDataTypes,
            columnIndexList,
            timeColumnDeletion,
            valueColumnsDeletionList,
            floatPrecision,
            encodingList,
            ignoreAllNullRows,
            maxNumberOfPointsInPage);
  }

  // MergeSortMultiAlignedTVListIterator
  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        null,
        Ordering.ASC,
        null,
        null,
        null,
        null,
        null,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        null,
        Ordering.ASC,
        null,
        timeColumnDeletion,
        valueColumnsDeletionList,
        null,
        null,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
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
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        tvListRowCounts,
        scanOrder,
        globalTimeFilter,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  // OrderedMultiAlignedTVListIterator
  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        null,
        Ordering.ASC,
        null,
        null,
        null,
        null,
        null,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        null,
        Ordering.ASC,
        null,
        timeColumnDeletion,
        valueColumnsDeletionList,
        null,
        null,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
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
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        tvListRowCounts,
        scanOrder,
        globalTimeFilter,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  public static MemPointIterator create(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    if (tvLists.size() == 1) {
      return single(tvLists, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(tvLists, null)) {
      return ordered(tsDataType, tvLists, maxNumberOfPointsInPage);
    } else {
      return mergeSort(tsDataType, tvLists, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      int maxNumberOfPointsInPage) {
    if (tvLists.size() == 1) {
      return single(tvLists, deletionList, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(tvLists, null)) {
      return ordered(tsDataType, tvLists, deletionList, maxNumberOfPointsInPage);
    } else {
      return mergeSort(tsDataType, tvLists, deletionList, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    if (tvLists.size() == 1) {
      return single(
          tvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          deletionList,
          floatPrecision,
          encoding,
          maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(tvLists, tvListRowCounts)) {
      return ordered(
          tsDataType,
          tvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          deletionList,
          floatPrecision,
          encoding,
          maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataType,
          tvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          deletionList,
          floatPrecision,
          encoding,
          maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    if (alignedTvLists.size() == 1) {
      return single(
          tsDataTypes, columnIndexList, alignedTvLists, ignoreAllNullRows, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists, null)) {
      return ordered(
          tsDataTypes, columnIndexList, alignedTvLists, ignoreAllNullRows, maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataTypes, columnIndexList, alignedTvLists, ignoreAllNullRows, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    if (alignedTvLists.size() == 1) {
      return single(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists, null)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
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
    if (alignedTvLists.size() == 1) {
      return single(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          timeColumnDeletion,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists, tvListRowCounts)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          timeColumnDeletion,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          tvListRowCounts,
          scanOrder,
          globalTimeFilter,
          timeColumnDeletion,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows,
          maxNumberOfPointsInPage);
    }
  }

  private static boolean isCompleteOrdered(
      List<? extends TVList> tvLists, List<Integer> tvListRowCounts) {
    long time = Long.MIN_VALUE;
    for (int i = 0; i < tvLists.size(); i++) {
      TVList list = tvLists.get(i);
      int rowCount = tvListRowCounts == null ? list.rowCount() : tvListRowCounts.get(i);

      if (rowCount == 0) {
        continue;
      }
      if (list.seqRowCount() < rowCount) {
        return false;
      }

      if (i > 0 && list.getTime(0) <= time) {
        return false;
      }
      time = list.getTime(rowCount - 1);
    }
    return true;
  }
}
