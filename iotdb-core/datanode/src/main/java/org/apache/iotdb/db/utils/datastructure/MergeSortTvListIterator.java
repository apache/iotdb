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

import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeSortTvListIterator implements IPointReader {
  private final List<TVList.TVListIterator> tvListIterators;
  private final TSDataType tsDataType;
  private final TSEncoding encoding;
  private final int floatPrecision;

  private TimeValuePair currentTvPair;

  public MergeSortTvListIterator(TSDataType tsDataType, List<TVList> tvLists) {
    this.tsDataType = tsDataType;
    this.encoding = null;
    this.floatPrecision = -1;
    tvListIterators = new ArrayList<>();
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator());
    }
  }

  public MergeSortTvListIterator(
      TSDataType tsDataType, TSEncoding encoding, int floatPrecision, List<TVList> tvLists) {
    this.tsDataType = tsDataType;
    this.encoding = encoding;
    this.floatPrecision = floatPrecision;
    tvListIterators = new ArrayList<>();
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator());
    }
  }

  private int getSelectedTVListIndex() {
    long time = Long.MAX_VALUE;
    int selectedTVListIndex = -1;
    for (int i = 0; i < tvListIterators.size(); i++) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      TimeValuePair currTvPair = null;
      if (iterator.hasNext()) {
        currTvPair = iterator.current();
      }

      // update minimum time and remember selected TVList
      if (currTvPair != null && currTvPair.getTimestamp() <= time) {
        time = currTvPair.getTimestamp();
        selectedTVListIndex = i;
      }
    }
    return selectedTVListIndex;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    boolean hasNext = false;
    int selectedTVListIndex = getSelectedTVListIndex();
    if (selectedTVListIndex >= 0) {
      currentTvPair = tvListIterators.get(selectedTVListIndex).next();
      hasNext = true;

      // call next to skip identical timestamp in other iterators
      for (TVList.TVListIterator iterator : tvListIterators) {
        TimeValuePair tvPair = iterator.current();
        if (tvPair != null && tvPair.getTimestamp() == currentTvPair.getTimestamp()) {
          iterator.next();
        }
      }
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    return currentTimeValuePair();
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (encoding != null && floatPrecision != -1) {
      if (tsDataType == TSDataType.FLOAT) {
        float value = currentTvPair.getValue().getFloat();
        if (!Float.isNaN(value)
            && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
          currentTvPair
              .getValue()
              .setFloat(MathUtils.roundWithGivenPrecision(value, floatPrecision));
        }
      } else if (tsDataType == TSDataType.DOUBLE) {
        double value = currentTvPair.getValue().getDouble();
        if (!Double.isNaN(value)
            && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
          currentTvPair
              .getValue()
              .setDouble(MathUtils.roundWithGivenPrecision(value, floatPrecision));
        }
      }
    }
    return currentTvPair;
  }

  @Override
  public long getUsedMemorySize() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    tvListIterators.clear();
  }

  public int[] getTVListIteratorOffsets() {
    int size = tvListIterators.size();
    int[] tvListsIndex = new int[size];
    for (int i = 0; i < size; i++) {
      tvListsIndex[i] = tvListIterators.get(i).getIndex();
    }
    return tvListsIndex;
  }

  public void setTVListIteratorOffsets(int[] tvListsIndex) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListsIndex[i]);
    }
  }
}
