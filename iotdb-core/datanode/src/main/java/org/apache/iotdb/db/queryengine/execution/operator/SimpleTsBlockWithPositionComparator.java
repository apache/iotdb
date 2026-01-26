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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SimpleTsBlockWithPositionComparator implements TsBlockWithPositionComparator {
  private final Comparator<SortKey> comparator;

  public SimpleTsBlockWithPositionComparator(
      List<TSDataType> types, List<Integer> sortChannels, List<SortOrder> sortOrders) {
    List<TSDataType> sortedTypes = new ArrayList<>();
    for (Integer sortChannel : sortChannels) {
      sortedTypes.add(types.get(sortChannel));
    }
    this.comparator =
        MergeSortComparator.getComparatorForTable(sortOrders, sortChannels, sortedTypes);
  }

  @Override
  public int compareTo(TsBlock left, int leftPosition, TsBlock right, int rightPosition) {
    return comparator.compare(
        new MergeSortKey(left, leftPosition), new MergeSortKey(right, rightPosition));
  }
}
