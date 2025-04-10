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
import org.apache.tsfile.read.common.TimeRange;

import java.util.List;

public class MemPointIteratorFactory {
  private MemPointIteratorFactory() {
    // forbidden construction
  }

  // TVListIterator
  private static MemPointIterator single(List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return tvLists.get(0).iterator(null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TVList> tvLists, List<TimeRange> deletionList, int maxNumberOfPointsInPage) {
    return tvLists.get(0).iterator(deletionList, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return tvLists.get(0).iterator(deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
  }

  // MergeSortMultiTVListIterator
  private static MemPointIterator mergeSort(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        tsDataType, tvLists, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        tsDataType, tvLists, deletionList, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiTVListIterator(
        tsDataType, tvLists, deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
  }

  // OrderedMultiTVListIterator
  private static MemPointIterator ordered(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        tsDataType, tvLists, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        tsDataType, tvLists, deletionList, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiTVListIterator(
        tsDataType, tvLists, deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
  }

  // AlignedTVListIterator
  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      int maxNumberOfPointsInPage) {
    return alignedTvLists
        .get(0)
        .iterator(tsDataTypes, columnIndexList, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      int maxNumberOfPointsInPage) {
    return alignedTvLists
        .get(0)
        .iterator(
            tsDataTypes,
            columnIndexList,
            valueColumnsDeletionList,
            null,
            null,
            maxNumberOfPointsInPage);
  }

  private static MemPointIterator single(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      int maxNumberOfPointsInPage) {
    return alignedTvLists
        .get(0)
        .iterator(
            tsDataTypes,
            columnIndexList,
            valueColumnsDeletionList,
            floatPrecision,
            encodingList,
            maxNumberOfPointsInPage);
  }

  // MergeSortMultiAlignedTVListIterator
  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        null,
        null,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      int maxNumberOfPointsInPage) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        maxNumberOfPointsInPage);
  }

  // OrderedMultiAlignedTVListIterator
  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null, maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        null,
        null,
        maxNumberOfPointsInPage);
  }

  private static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      int maxNumberOfPointsInPage) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        maxNumberOfPointsInPage);
  }

  public static MemPointIterator create(
      TSDataType tsDataType, List<TVList> tvLists, int maxNumberOfPointsInPage) {
    if (tvLists.size() == 1) {
      return single(tvLists, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(tvLists)) {
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
    } else if (isCompleteOrdered(tvLists)) {
      return ordered(tsDataType, tvLists, deletionList, maxNumberOfPointsInPage);
    } else {
      return mergeSort(tsDataType, tvLists, deletionList, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding,
      int maxNumberOfPointsInPage) {
    if (tvLists.size() == 1) {
      return single(tvLists, deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(tvLists)) {
      return ordered(
          tsDataType, tvLists, deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataType, tvLists, deletionList, floatPrecision, encoding, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      int maxNumberOfPointsInPage) {
    if (alignedTvLists.size() == 1) {
      return single(tsDataTypes, columnIndexList, alignedTvLists, maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists)) {
      return ordered(tsDataTypes, columnIndexList, alignedTvLists, maxNumberOfPointsInPage);
    } else {
      return mergeSort(tsDataTypes, columnIndexList, alignedTvLists, maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      int maxNumberOfPointsInPage) {
    if (alignedTvLists.size() == 1) {
      return single(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          maxNumberOfPointsInPage);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      int maxNumberOfPointsInPage) {
    if (alignedTvLists.size() == 1) {
      return single(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          maxNumberOfPointsInPage);
    } else if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          maxNumberOfPointsInPage);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          maxNumberOfPointsInPage);
    }
  }

  private static boolean isCompleteOrdered(List<? extends TVList> tvLists) {
    long time = Long.MIN_VALUE;
    for (int i = 0; i < tvLists.size(); i++) {
      TVList list = tvLists.get(i);
      if (!list.isSorted()) {
        return false;
      }

      if (tvLists.get(i).rowCount() == 0) {
        continue;
      }
      if (i > 0 && list.getTime(0) <= time) {
        return false;
      }
      time = list.getTime(list.rowCount() - 1);
    }
    return true;
  }
}
