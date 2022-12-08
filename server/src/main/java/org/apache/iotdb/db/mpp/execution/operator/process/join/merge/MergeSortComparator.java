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

import java.util.Comparator;
import java.util.List;

public class MergeSortComparator {

  public static final Comparator<MergeSortKey> ASC_TIME_ASC_DEVICE =
      getTimeComparator(Ordering.ASC, Ordering.ASC);
  public static final Comparator<MergeSortKey> ASC_TIME_DESC_DEVICE =
      getTimeComparator(Ordering.ASC, Ordering.DESC);
  public static final Comparator<MergeSortKey> DESC_TIME_ASC_DEVICE =
      getTimeComparator(Ordering.DESC, Ordering.ASC);
  public static final Comparator<MergeSortKey> DESC_TIME_DESC_DEVICE =
      getTimeComparator(Ordering.DESC, Ordering.DESC);

  public static final Comparator<MergeSortKey> ASC_DEVICE_ASC_TIME =
      getDeviceComparator(Ordering.ASC, Ordering.ASC);
  public static final Comparator<MergeSortKey> ASC_DEVICE_DESC_TIME =
      getDeviceComparator(Ordering.ASC, Ordering.DESC);
  public static final Comparator<MergeSortKey> DESC_DEVICE_ASC_TIME =
      getDeviceComparator(Ordering.DESC, Ordering.ASC);
  public static final Comparator<MergeSortKey> DESC_DEVICE_DESC_TIME =
      getDeviceComparator(Ordering.DESC, Ordering.DESC);

  private static Comparator<MergeSortKey> getTimeComparator(
      Ordering timeOrdering, Ordering deviceOrdering) {
    return (MergeSortKey o1, MergeSortKey o2) ->
        o1.tsBlock.getTimeByIndex(o1.rowIndex) == o2.tsBlock.getTimeByIndex(o2.rowIndex)
            ? (deviceOrdering == Ordering.ASC
                ? o1.tsBlock
                    .getColumn(0)
                    .getBinary(o1.rowIndex)
                    .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex))
                : o2.tsBlock
                    .getColumn(0)
                    .getBinary(o2.rowIndex)
                    .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex)))
            : (int)
                (timeOrdering == Ordering.ASC
                    ? o1.tsBlock.getTimeByIndex(o1.rowIndex)
                        - o2.tsBlock.getTimeByIndex(o2.rowIndex)
                    : o2.tsBlock.getTimeByIndex(o2.rowIndex)
                        - o1.tsBlock.getTimeByIndex(o1.rowIndex));
  }

  private static Comparator<MergeSortKey> getDeviceComparator(
      Ordering timeOrdering, Ordering deviceOrdering) {
    return (MergeSortKey o1, MergeSortKey o2) ->
        o1.tsBlock.getColumn(0).getBinary(o1.rowIndex)
                == o2.tsBlock.getColumn(0).getBinary(o2.rowIndex)
            ? (int)
                (timeOrdering == Ordering.ASC
                    ? o1.tsBlock.getTimeByIndex(o1.rowIndex)
                        - o2.tsBlock.getTimeByIndex(o2.rowIndex)
                    : o2.tsBlock.getTimeByIndex(o2.rowIndex)
                        - o1.tsBlock.getTimeByIndex(o1.rowIndex))
            : (deviceOrdering == Ordering.ASC
                ? o1.tsBlock
                    .getColumn(0)
                    .getBinary(o1.rowIndex)
                    .compareTo(o2.tsBlock.getColumn(0).getBinary(o2.rowIndex))
                : o2.tsBlock
                    .getColumn(0)
                    .getBinary(o2.rowIndex)
                    .compareTo(o1.tsBlock.getColumn(0).getBinary(o1.rowIndex)));
  }

  public static Comparator<MergeSortKey> getComparator(List<SortItem> sortItemList) {
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
  }
}
