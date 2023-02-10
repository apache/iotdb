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

package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.collections4.comparators.ComparatorChain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MergeSortComparator {

  public static final Comparator<MergeSortKey> ASC_TIME_ASC_DEVICE =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int timeComparing =
            (int) (o1.tsBlock.getTimeByIndex(o1.rowIndex) - o2.tsBlock.getTimeByIndex(o2.rowIndex));
        return timeComparing == 0
            ? o1.tsBlock
                .getColumn(0)
                .getBinary(o1.rowIndex)
                .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex))
            : timeComparing;
      };
  public static final Comparator<MergeSortKey> ASC_TIME_DESC_DEVICE =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int timeComparing =
            (int) (o1.tsBlock.getTimeByIndex(o1.rowIndex) - o2.tsBlock.getTimeByIndex(o2.rowIndex));
        return timeComparing == 0
            ? o2.tsBlock
                .getColumn(0)
                .getBinary(o2.rowIndex)
                .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex))
            : timeComparing;
      };
  public static final Comparator<MergeSortKey> DESC_TIME_ASC_DEVICE =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int timeComparing =
            (int) (o2.tsBlock.getTimeByIndex(o2.rowIndex) - o1.tsBlock.getTimeByIndex(o1.rowIndex));
        return timeComparing == 0
            ? o1.tsBlock
                .getColumn(0)
                .getBinary(o1.rowIndex)
                .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex))
            : timeComparing;
      };
  public static final Comparator<MergeSortKey> DESC_TIME_DESC_DEVICE =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int timeComparing =
            (int) (o2.tsBlock.getTimeByIndex(o2.rowIndex) - o1.tsBlock.getTimeByIndex(o1.rowIndex));
        return timeComparing == 0
            ? o2.tsBlock
                .getColumn(0)
                .getBinary(o2.rowIndex)
                .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex))
            : timeComparing;
      };

  public static final Comparator<MergeSortKey> ASC_DEVICE_ASC_TIME =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int deviceComparing =
            o1.tsBlock
                .getColumn(0)
                .getBinary(o1.rowIndex)
                .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex));
        return deviceComparing == 0
            ? (int)
                (o1.tsBlock.getTimeByIndex(o1.rowIndex) - o2.tsBlock.getTimeByIndex(o2.rowIndex))
            : deviceComparing;
      };
  public static final Comparator<MergeSortKey> ASC_DEVICE_DESC_TIME =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int deviceComparing =
            o1.tsBlock
                .getColumn(0)
                .getBinary(o1.rowIndex)
                .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex));
        return deviceComparing == 0
            ? (int)
                (o2.tsBlock.getTimeByIndex(o2.rowIndex) - o1.tsBlock.getTimeByIndex(o1.rowIndex))
            : deviceComparing;
      };
  public static final Comparator<MergeSortKey> DESC_DEVICE_ASC_TIME =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int deviceComparing =
            o2.tsBlock
                .getColumn(0)
                .getBinary(o2.rowIndex)
                .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex));
        return deviceComparing == 0
            ? (int)
                (o1.tsBlock.getTimeByIndex(o1.rowIndex) - o2.tsBlock.getTimeByIndex(o2.rowIndex))
            : deviceComparing;
      };
  public static final Comparator<MergeSortKey> DESC_DEVICE_DESC_TIME =
      (MergeSortKey o1, MergeSortKey o2) -> {
        int deviceComparing =
            o2.tsBlock
                .getColumn(0)
                .getBinary(o2.rowIndex)
                .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex));
        return deviceComparing == 0
            ? (int)
                (o2.tsBlock.getTimeByIndex(o2.rowIndex) - o1.tsBlock.getTimeByIndex(o1.rowIndex))
            : deviceComparing;
      };

  public static Comparator<MergeSortKey> getComparator(
      List<SortItem> sortItemList, List<Integer> indexList, List<TSDataType> dataTypeList) {
    // specified for order by time, device or order by device, time
    if (sortItemList.size() == 2
        && ((sortItemList.get(0).getSortKey() == SortKey.TIME
                && sortItemList.get(1).getSortKey() == SortKey.DEVICE)
            || (sortItemList.get(0).getSortKey() == SortKey.DEVICE
                && sortItemList.get(1).getSortKey() == SortKey.TIME))) {
      if (sortItemList.get(0).getOrdering() == Ordering.ASC) {
        if (sortItemList.get(1).getOrdering() == Ordering.ASC) {
          if (sortItemList.get(0).getSortKey() == SortKey.TIME) return ASC_TIME_ASC_DEVICE;
          else return ASC_DEVICE_ASC_TIME;
        } else {
          if (sortItemList.get(0).getSortKey() == SortKey.TIME) return ASC_TIME_DESC_DEVICE;
          else return ASC_DEVICE_DESC_TIME;
        }
      } else {
        if (sortItemList.get(1).getOrdering() == Ordering.ASC) {
          if (sortItemList.get(0).getSortKey() == SortKey.TIME) return DESC_TIME_ASC_DEVICE;
          else return DESC_DEVICE_ASC_TIME;
        } else {
          if (sortItemList.get(0).getSortKey() == SortKey.TIME) return DESC_TIME_DESC_DEVICE;
          else return DESC_DEVICE_DESC_TIME;
        }
      }
    } else { // generally order by
      // use code-gen compile this comparator

      // currently only show queries use merge sort and will only contain TIME, QUERYID, DATANODEID,
      // ELAPSEDTIME, STATEMENT
      return genComparatorChain(sortItemList, indexList, dataTypeList);
    }
  }

  /** @param indexList -1 for time column */
  private static ComparatorChain<MergeSortKey> genComparatorChain(
      List<SortItem> sortItemList, List<Integer> indexList, List<TSDataType> dataTypeList) {
    List<Comparator<MergeSortKey>> list = new ArrayList<>(indexList.size());
    for (int i = 0; i < indexList.size(); i++) {
      int index = indexList.get(i);
      TSDataType dataType = dataTypeList.get(i);
      boolean asc = sortItemList.get(i).getOrdering() == Ordering.ASC;
      list.add(genSingleComparator(asc, index, dataType));
    }
    return new ComparatorChain<>(list);
  }

  private static Comparator<MergeSortKey> genSingleComparator(
      boolean asc, int index, TSDataType dataType) {
    Comparator<MergeSortKey> comparator;
    switch (dataType) {
      case INT32:
        comparator =
            Comparator.comparingInt(
                (MergeSortKey sortKey) ->
                    sortKey.tsBlock.getColumn(index).getInt(sortKey.rowIndex));
        break;
      case INT64:
        if (index == -1) {
          comparator =
              Comparator.comparingLong(
                  (MergeSortKey sortKey) -> sortKey.tsBlock.getTimeByIndex(sortKey.rowIndex));
        } else {
          comparator =
              Comparator.comparingLong(
                  (MergeSortKey sortKey) ->
                      sortKey.tsBlock.getColumn(index).getLong(sortKey.rowIndex));
        }
        break;
      case FLOAT:
        comparator =
            Comparator.comparingDouble(
                (MergeSortKey sortKey) ->
                    sortKey.tsBlock.getColumn(index).getFloat(sortKey.rowIndex));
        break;
      case DOUBLE:
        comparator =
            Comparator.comparingDouble(
                (MergeSortKey sortKey) ->
                    sortKey.tsBlock.getColumn(index).getDouble(sortKey.rowIndex));
        break;
      case TEXT:
        comparator =
            Comparator.comparing(
                (MergeSortKey sortKey) ->
                    sortKey.tsBlock.getColumn(index).getBinary(sortKey.rowIndex));
        break;
      default:
        throw new IllegalArgumentException("Data type: " + dataType + " cannot be ordered");
    }
    if (!asc) {
      comparator = comparator.reversed();
    }
    return comparator;
  }
}
