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

import org.apache.iotdb.db.utils.datastructure.SortKey;

import java.util.Comparator;

public class SortKeyComparator implements Comparator<SortKey> {

  private final boolean nullFirst;
  private final int index;
  private final Comparator<SortKey> originalComparator;

  public SortKeyComparator(int index, boolean nullFirst, Comparator<SortKey> originalComparator) {
    this.nullFirst = nullFirst;
    this.index = index;
    this.originalComparator = originalComparator;
  }

  @Override
  public int compare(SortKey o1, SortKey o2) {
    boolean o1IsNull = o1.tsBlock.getColumn(index).isNull(o1.rowIndex);
    boolean o2IsNull = o2.tsBlock.getColumn(index).isNull(o2.rowIndex);

    if (!o1IsNull && !o2IsNull) {
      return originalComparator.compare(o1, o2);
    } else if (o1IsNull) {
      if (o2IsNull) {
        return 0;
      } else {
        return nullFirst ? -1 : 1;
      }
    } else {
      return nullFirst ? 1 : -1;
    }
  }
}
