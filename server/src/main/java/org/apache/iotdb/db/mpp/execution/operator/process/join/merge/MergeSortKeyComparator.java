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

import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MergeSortKeyComparator implements Comparator<MergeSortKey>, Serializable {

  private final boolean nullFirst;
  private final int index;
  private final Comparator<MergeSortKey> originalComparator;
  private static final Map<
          Pair<Comparator<MergeSortKey>, Pair<Boolean, Integer>>, MergeSortKeyComparator>
      comparatorCache = new ConcurrentHashMap<>();

  public MergeSortKeyComparator(
      int index, boolean nullFirst, Comparator<MergeSortKey> originalComparator) {
    this.nullFirst = nullFirst;
    this.index = index;
    this.originalComparator = originalComparator;
  }

  public static MergeSortKeyComparator getInstance(
      int index, boolean nullFirst, Comparator<MergeSortKey> originalComparator) {
    return comparatorCache.computeIfAbsent(
        new Pair<>(originalComparator, new Pair<>(nullFirst, index)),
        comparator -> new MergeSortKeyComparator(index, nullFirst, originalComparator));
  }

  @Override
  public int compare(MergeSortKey o1, MergeSortKey o2) {
    boolean o1IsNull = o1.tsBlock.getColumn(index).isNull(o1.rowIndex);
    boolean o2IsNull = o2.tsBlock.getColumn(index).isNull(o2.rowIndex);

    if (!o1IsNull && !o2IsNull) {
      return originalComparator.compare(o1, o2);
    } else if (o1IsNull) {
      return o2IsNull ? 0 : (nullFirst ? -1 : 1);
    } else {
      return nullFirst ? 1 : -1;
    }
  }

  @Override
  public Comparator<MergeSortKey> thenComparing(Comparator<? super MergeSortKey> other) {
    Objects.requireNonNull(other);
    return new MergeSortKeyComparator(index, nullFirst, originalComparator.thenComparing(other));
  }

  @Override
  public Comparator<MergeSortKey> reversed() {
    return new MergeSortKeyComparator(index, !nullFirst, originalComparator.reversed());
  }
}
