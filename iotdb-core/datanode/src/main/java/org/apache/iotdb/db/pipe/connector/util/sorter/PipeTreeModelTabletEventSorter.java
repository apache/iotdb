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

package org.apache.iotdb.db.pipe.connector.util.sorter;

import org.apache.tsfile.write.record.Tablet;

import java.util.Arrays;
import java.util.Comparator;

public class PipeTreeModelTabletEventSorter extends PipeTabletEventSorter {

  public PipeTreeModelTabletEventSorter(final Tablet tablet) {
    super(tablet);
    deduplicateSize = tablet == null ? 0 : tablet.getRowSize();
  }

  public void deduplicateAndSortTimestampsIfNecessary() {
    if (tablet == null || tablet.getRowSize() == 0) {
      return;
    }

    long[] timestamps = tablet.getTimestamps();
    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final long currentTimestamp = timestamps[i];
      final long previousTimestamp = timestamps[i - 1];

      if (currentTimestamp < previousTimestamp) {
        isSorted = false;
        break;
      }
      if (currentTimestamp == previousTimestamp) {
        isDeduplicate = false;
      }
    }

    if (isSorted && isDeduplicate) {
      return;
    }

    index = new Integer[tablet.getRowSize()];
    for (int i = 0, size = tablet.getRowSize(); i < size; i++) {
      index[i] = i;
    }

    if (!isSorted) {
      sortTimestamps();

      // Do deduplicate anyway.
      // isDeduplicated may be false positive when isSorted is false.
      deduplicateTimestamps();
      isDeduplicate = true;
    }

    if (!isDeduplicate) {
      deduplicateTimestamps();
    }

    sortAndDeduplicateValuesAndBitMaps();
  }

  private void sortTimestamps() {
    Arrays.sort(index, Comparator.comparingLong(tablet::getTimestamp));
    Arrays.sort(tablet.getTimestamps(), 0, tablet.getRowSize());
  }

  private void deduplicateTimestamps() {
    deduplicateSize = 0;
    long[] timestamps = tablet.getTimestamps();
    for (int i = 1, size = tablet.getRowSize(); i < size; i++) {
      if (timestamps[i] != timestamps[i - 1]) {
        deduplicateIndex[deduplicateSize] = i - 1;
        timestamps[deduplicateSize] = timestamps[i - 1];

        ++deduplicateSize;
      }
    }

    deduplicateIndex[deduplicateSize] = tablet.getRowSize() - 1;
    tablet.setRowSize(deduplicateSize + 1);
  }
}
