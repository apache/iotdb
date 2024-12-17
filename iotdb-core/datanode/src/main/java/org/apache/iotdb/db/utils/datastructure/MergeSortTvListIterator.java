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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.List;

public class MergeSortTvListIterator implements IPointReader {
  private final TVList.TVListIterator[] tvListIterators;

  private int selectedTVListIndex = -1;
  private TimeValuePair currentTvPair;

  private final int[] tvListOffsets;

  public MergeSortTvListIterator(List<TVList> tvLists) {
    tvListIterators = new TVList.TVListIterator[tvLists.size()];
    for (int i = 0; i < tvLists.size(); i++) {
      tvListIterators[i] = tvLists.get(i).iterator(null, null);
    }
    this.tvListOffsets = new int[tvLists.size()];
  }

  public MergeSortTvListIterator(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    tvListIterators = new TVList.TVListIterator[tvLists.size()];
    for (int i = 0; i < tvLists.size(); i++) {
      tvListIterators[i] = tvLists.get(i).iterator(floatPrecision, encoding);
    }
    this.tvListOffsets = new int[tvLists.size()];
  }

  private void prepareNextRow() {
    long time = Long.MAX_VALUE;
    selectedTVListIndex = -1;
    for (int i = 0; i < tvListIterators.length; i++) {
      TVList.TVListIterator iterator = tvListIterators[i];
      boolean hasNext = iterator.hasNext();
      // update minimum time and remember selected TVList
      if (hasNext && iterator.currentTime() <= time) {
        time = iterator.currentTime();
        selectedTVListIndex = i;
      }
    }
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (selectedTVListIndex == -1) {
      prepareNextRow();
    }
    return selectedTVListIndex >= 0 && selectedTVListIndex < tvListIterators.length;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    currentTvPair = tvListIterators[selectedTVListIndex].next();
    tvListOffsets[selectedTVListIndex] = tvListIterators[selectedTVListIndex].getIndex();

    // call next to skip identical timestamp in other iterators
    for (int i = 0; i < tvListIterators.length; i++) {
      if (selectedTVListIndex == i) {
        continue;
      }
      TVList.TVListIterator iterator = tvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == currentTvPair.getTimestamp()) {
        tvListIterators[i].step();
        tvListOffsets[i] = tvListIterators[i].getIndex();
      }
    }

    selectedTVListIndex = -1;
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return currentTvPair;
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {}

  public int[] getTVListOffsets() {
    return tvListOffsets;
  }

  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.length; i++) {
      tvListIterators[i].setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    selectedTVListIndex = -1;
  }
}
