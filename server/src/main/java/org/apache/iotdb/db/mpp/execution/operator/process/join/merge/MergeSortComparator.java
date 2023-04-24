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

import org.apache.iotdb.db.mpp.plan.statement.component.NullOrdering;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.collections4.comparators.ComparatorChain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MergeSortComparator {

  /** @param indexList -1 for time column */
  public static Comparator<MergeSortKey> getComparator(
      List<SortItem> sortItemList, List<Integer> indexList, List<TSDataType> dataTypeList) {

    // use code-gen compile this comparator
    List<Comparator<MergeSortKey>> list = new ArrayList<>(indexList.size());
    for (int i = 0; i < indexList.size(); i++) {
      int index = indexList.get(i);
      TSDataType dataType = dataTypeList.get(i);
      boolean asc = sortItemList.get(i).getOrdering() == Ordering.ASC;
      boolean nullFirst = sortItemList.get(i).getNullOrdering() == NullOrdering.FIRST;
      list.add(genSingleComparator(asc, index, dataType, nullFirst));
    }
    return new ComparatorChain<>(list);
  }

  private static Comparator<MergeSortKey> genSingleComparator(
      boolean asc, int index, TSDataType dataType, boolean nullFirst) {
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
      case BOOLEAN:
        comparator =
            Comparator.comparing(
                (MergeSortKey sortKey) ->
                    sortKey.tsBlock.getColumn(index).getBoolean(sortKey.rowIndex));
        break;
      default:
        throw new IllegalArgumentException("Data type: " + dataType + " cannot be ordered");
    }
    if (!asc) {
      comparator = comparator.reversed();
    }

    return index != -1
        ? MergeSortKeyComparator.getInstance(index, nullFirst, comparator)
        : comparator;
  }
}
