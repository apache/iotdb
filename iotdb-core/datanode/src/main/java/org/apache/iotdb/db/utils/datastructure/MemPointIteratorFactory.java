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

  // MergeSortMultiTVListIterator
  public static MemPointIterator mergeSort(TSDataType tsDataType, List<TVList> tvLists) {
    return new MergeSortMultiTVListIterator(tsDataType, tvLists, null, null, null);
  }

  public static MemPointIterator mergeSort(
      TSDataType tsDataType, List<TVList> tvLists, List<TimeRange> deletionList) {
    return new MergeSortMultiTVListIterator(tsDataType, tvLists, deletionList, null, null);
  }

  public static MemPointIterator mergeSort(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    return new MergeSortMultiTVListIterator(
        tsDataType, tvLists, deletionList, floatPrecision, encoding);
  }

  // OrderedMultiTVListIterator
  public static MemPointIterator ordered(TSDataType tsDataType, List<TVList> tvLists) {
    return new OrderedMultiTVListIterator(tsDataType, tvLists, null, null, null);
  }

  public static MemPointIterator ordered(
      TSDataType tsDataType, List<TVList> tvLists, List<TimeRange> deletionList) {
    return new OrderedMultiTVListIterator(tsDataType, tvLists, deletionList, null, null);
  }

  public static MemPointIterator ordered(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    return new OrderedMultiTVListIterator(
        tsDataType, tvLists, deletionList, floatPrecision, encoding);
  }

  // MergeSortMultiAlignedTVListIterator
  public static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null);
  }

  public static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, valueColumnsDeletionList, null, null);
  }

  public static MemPointIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList);
  }

  // OrderedMultiAlignedTVListIterator
  public static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null);
  }

  public static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, valueColumnsDeletionList, null, null);
  }

  public static MemPointIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList);
  }

  public static MemPointIterator create(TSDataType tsDataType, List<TVList> tvLists) {
    if (isCompleteOrdered(tvLists)) {
      return ordered(tsDataType, tvLists);
    } else {
      return mergeSort(tsDataType, tvLists);
    }
  }

  public static MemPointIterator create(
      TSDataType tsDataType, List<TVList> tvLists, List<TimeRange> deletionList) {
    if (isCompleteOrdered(tvLists)) {
      return ordered(tsDataType, tvLists, deletionList);
    } else {
      return mergeSort(tsDataType, tvLists, deletionList);
    }
  }

  public static MemPointIterator create(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    if (isCompleteOrdered(tvLists)) {
      return ordered(tsDataType, tvLists, deletionList, floatPrecision, encoding);
    } else {
      return mergeSort(tsDataType, tvLists, deletionList, floatPrecision, encoding);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(tsDataTypes, columnIndexList, alignedTvLists);
    } else {
      return mergeSort(tsDataTypes, columnIndexList, alignedTvLists);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(tsDataTypes, columnIndexList, alignedTvLists, valueColumnsDeletionList);
    } else {
      return mergeSort(tsDataTypes, columnIndexList, alignedTvLists, valueColumnsDeletionList);
    }
  }

  public static MemPointIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList);
    }
  }

  private static boolean isCompleteOrdered(List<? extends TVList> tvLists) {
    long time = Long.MIN_VALUE;
    for (TVList list : tvLists) {
      if (list.rowCount() == 0) {
        continue;
      }
      if (!list.isSorted() || list.getTime(0) <= time) {
        return false;
      }
      time = list.getTime(list.rowCount() - 1);
    }
    return true;
  }
}
