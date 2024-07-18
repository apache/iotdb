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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge;

import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.statement.component.NullOrdering;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.commons.collections4.comparators.ComparatorChain;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MergeSortComparator {

  private static final Comparator<SortKey> TIME_ASC_COMPARATOR =
      Comparator.comparingLong(
          (SortKey sortKey) -> sortKey.tsBlock.getTimeByIndex(sortKey.rowIndex));

  private static final Comparator<SortKey> TIME_DESC_COMPARATOR =
      Comparator.comparingLong(
              (SortKey sortKey) -> sortKey.tsBlock.getTimeByIndex(sortKey.rowIndex))
          .reversed();

  private MergeSortComparator() {
    // util class doesn't need constructor
  }

  /** -1 in index is for time column. */
  public static Comparator<SortKey> getComparator(
      List<SortItem> sortItemList, List<Integer> indexList, List<TSDataType> dataTypeList) {

    // use code-gen compile this comparator
    List<Comparator<SortKey>> list = new ArrayList<>(indexList.size());
    for (int i = 0; i < indexList.size(); i++) {
      int index = indexList.get(i);
      if (index == -2) {
        continue;
      }
      TSDataType dataType = dataTypeList.get(i);
      boolean asc = sortItemList.get(i).getOrdering() == Ordering.ASC;
      boolean nullFirst = sortItemList.get(i).getNullOrdering() == NullOrdering.FIRST;
      list.add(genSingleComparator(asc, index, dataType, nullFirst));
    }

    return list.size() == 1 ? list.get(0) : new ComparatorChain<>(list);
  }

  public static Comparator<SortKey> getComparatorForTable(
      List<SortOrder> sortOrderList, List<Integer> indexList, List<TSDataType> dataTypeList) {

    // use code-gen compile this comparator
    List<Comparator<SortKey>> list = new ArrayList<>(indexList.size());
    for (int i = 0; i < indexList.size(); i++) {
      int index = indexList.get(i);
      if (index == -2) {
        continue;
      }
      TSDataType dataType = dataTypeList.get(i);
      SortOrder sortOrder = sortOrderList.get(i);
      list.add(
          genSingleComparator(sortOrder.isAscending(), index, dataType, sortOrder.isNullsFirst()));
    }

    return list.size() == 1 ? list.get(0) : new ComparatorChain<>(list);
  }

  public static Comparator<SortKey> getComparator(TSDataType dataType, int index, boolean asc) {
    Comparator<SortKey> comparator;
    switch (dataType) {
      case INT32:
      case DATE:
        comparator =
            Comparator.comparingInt(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getInt(sortKey.rowIndex));
        break;
      case INT64:
      case TIMESTAMP:
        comparator =
            Comparator.comparingLong(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getLong(sortKey.rowIndex));
        break;
      case FLOAT:
        comparator =
            Comparator.comparingDouble(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getFloat(sortKey.rowIndex));
        break;
      case DOUBLE:
        comparator =
            Comparator.comparingDouble(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getDouble(sortKey.rowIndex));
        break;
      case TEXT:
      case BLOB:
      case STRING:
        comparator =
            Comparator.comparing(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getBinary(sortKey.rowIndex));
        break;
      case BOOLEAN:
        comparator =
            Comparator.comparing(
                (SortKey sortKey) -> sortKey.tsBlock.getColumn(index).getBoolean(sortKey.rowIndex));
        break;
      default:
        throw new IllegalArgumentException("Data type: " + dataType + " cannot be ordered");
    }
    if (!asc) {
      comparator = comparator.reversed();
    }

    return comparator;
  }

  private static Comparator<SortKey> genSingleComparator(
      boolean asc, int index, TSDataType dataType, boolean nullFirst) {

    if (index == -1) {
      return asc ? TIME_ASC_COMPARATOR : TIME_DESC_COMPARATOR;
    }
    return new SortKeyComparator(index, nullFirst, getComparator(dataType, index, asc));
  }
}
