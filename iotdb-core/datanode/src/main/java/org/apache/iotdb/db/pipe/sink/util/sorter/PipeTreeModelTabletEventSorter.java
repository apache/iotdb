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

package org.apache.iotdb.db.pipe.sink.util.sorter;

import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.write.record.Tablet;

import java.util.Arrays;
import java.util.Comparator;

public class PipeTreeModelTabletEventSorter extends PipeInsertEventSorter {

  /**
   * Constructor for Tablet.
   *
   * @param tablet the tablet to sort
   */
  public PipeTreeModelTabletEventSorter(final Tablet tablet) {
    super(tablet);
    deDuplicatedSize = tablet == null ? 0 : tablet.getRowSize();
  }

  /**
   * Constructor for InsertTabletStatement.
   *
   * @param statement the insert tablet statement to sort
   */
  public PipeTreeModelTabletEventSorter(final InsertTabletStatement statement) {
    super(statement);
    deDuplicatedSize = statement == null ? 0 : statement.getRowCount();
  }

  public void deduplicateAndSortTimestampsIfNecessary() {
    if (dataAdapter == null || dataAdapter.getRowSize() == 0) {
      return;
    }

    long[] timestamps = dataAdapter.getTimestamps();
    final int rowSize = dataAdapter.getRowSize();
    for (int i = 1; i < rowSize; ++i) {
      final long currentTimestamp = timestamps[i];
      final long previousTimestamp = timestamps[i - 1];

      if (currentTimestamp < previousTimestamp) {
        isSorted = false;
        break;
      }
      if (currentTimestamp == previousTimestamp) {
        isDeDuplicated = false;
      }
    }

    if (isSorted && isDeDuplicated) {
      return;
    }

    index = new Integer[rowSize];
    deDuplicatedIndex = new int[rowSize];
    for (int i = 0; i < rowSize; i++) {
      index[i] = i;
    }

    if (!isSorted) {
      sortTimestamps();

      // Do deDuplicated anyway.
      // isDeDuplicated may be false positive when isSorted is false.
      deduplicateTimestamps();
      isDeDuplicated = true;
    }

    if (!isDeDuplicated) {
      deduplicateTimestamps();
    }

    sortAndMayDeduplicateValuesAndBitMaps();
  }

  private void sortTimestamps() {
    // Index is sorted stably because it is Integer[]
    Arrays.sort(index, Comparator.comparingLong(dataAdapter::getTimestamp));
    final long[] timestamps = dataAdapter.getTimestamps();
    Arrays.sort(timestamps, 0, dataAdapter.getRowSize());
  }

  private void deduplicateTimestamps() {
    deDuplicatedSize = 0;
    long[] timestamps = dataAdapter.getTimestamps();
    final int rowSize = dataAdapter.getRowSize();
    for (int i = 1; i < rowSize; i++) {
      if (timestamps[i] != timestamps[i - 1]) {
        deDuplicatedIndex[deDuplicatedSize] = i - 1;
        timestamps[deDuplicatedSize] = timestamps[i - 1];

        ++deDuplicatedSize;
      }
    }

    deDuplicatedIndex[deDuplicatedSize] = rowSize - 1;
    timestamps[deDuplicatedSize] = timestamps[rowSize - 1];
    dataAdapter.setRowSize(++deDuplicatedSize);
  }
}
