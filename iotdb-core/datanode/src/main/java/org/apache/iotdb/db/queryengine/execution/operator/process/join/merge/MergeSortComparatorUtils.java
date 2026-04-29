/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge;

import org.apache.iotdb.calc.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.NullOrdering;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.collections4.comparators.ComparatorChain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MergeSortComparatorUtils {
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
      list.add(MergeSortComparator.genSingleComparator(asc, index, dataType, nullFirst));
    }

    return list.size() == 1 ? list.get(0) : new ComparatorChain<>(list);
  }
}
