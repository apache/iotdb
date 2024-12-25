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

package org.apache.iotdb.db.queryengine.transformation.datastructure.iterator;

import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.SerializableTVList;

import org.apache.tsfile.block.column.Column;

import java.io.IOException;

// Forward iterator used in ElasticSerializableTVList
// Point to columns(time column and value column)
public class TVListForwardIterator implements ListForwardIterator {
  private final ElasticSerializableTVList tvList;

  private int externalIndex; // Which SerializableTVList
  private int internalIndex; // Which columns in SerializableTVList

  // In case of tvList changing
  private int endPointIndex; // Index of last point of the columns(open)

  public TVListForwardIterator(ElasticSerializableTVList tvList) {
    this.tvList = tvList;
    // Point to dummy block for simplicity
    externalIndex = 0;
    internalIndex = -1;
    endPointIndex = 0;
  }

  public TVListForwardIterator(
      ElasticSerializableTVList tvList, int externalIndex, int internalIndex) throws IOException {
    this.tvList = tvList;
    this.externalIndex = externalIndex;
    this.internalIndex = internalIndex;
    endPointIndex = tvList.getLastPointIndex(externalIndex, internalIndex);
  }

  public Column currentTimes() throws IOException {
    return tvList.getTimeColumn(externalIndex, internalIndex);
  }

  public Column currentValues() throws IOException {
    return tvList.getValueColumn(externalIndex, internalIndex);
  }

  @Override
  public boolean hasNext() throws IOException {
    // First time call, tvList has no data
    if (tvList.getSerializableTVListSize() == 0) {
      return false;
    }

    return externalIndex + 1 < tvList.getSerializableTVListSize()
        || internalIndex + 1 < tvList.getSerializableTVList(externalIndex).getColumnCount();
  }

  @Override
  public void next() throws IOException {
    // Move forward iterator
    if (internalIndex + 1 == tvList.getColumnCount(externalIndex)) {
      internalIndex = 0;
      externalIndex++;
    } else {
      internalIndex++;
    }

    // Assume we already consume all data in this block
    SerializableTVList internalTVList = tvList.getSerializableTVList(externalIndex);
    endPointIndex += internalTVList.getColumnSize(internalIndex);
  }

  // When tvList apply new memory control strategy, the origin iterators become invalid.
  // We can relocate these old iterators by its startPointIndex
  public void adjust() throws IOException {
    int capacity = tvList.getInternalTVListCapacity();

    int externalColumnIndex = endPointIndex / capacity;
    int internalPointIndex = endPointIndex % capacity;
    // endPointIndex is not closed, i.e. endPointIndex)
    int internalColumnIndex =
        tvList.getSerializableTVList(externalIndex).getColumnIndex(internalPointIndex - 1);

    this.externalIndex = externalColumnIndex;
    this.internalIndex = internalColumnIndex;
  }

  public int getEndPointIndex() {
    return endPointIndex;
  }
}
