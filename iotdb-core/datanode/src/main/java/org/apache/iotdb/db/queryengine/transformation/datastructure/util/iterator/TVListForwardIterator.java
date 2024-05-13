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

package org.apache.iotdb.db.queryengine.transformation.datastructure.util.iterator;

import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.queryengine.transformation.datastructure.tv.SerializableTVList;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.io.IOException;
import java.util.List;

public class TVListForwardIterator implements ListForwardIterator {
  private ElasticSerializableTVList tvList;

  private int externalIndex; // Which tvList
  private int internalIndex; // Which block in tvList

  // Cached in case of TVList's change
  private TimeColumn cachedTimeColumn;
  private Column cachedValueColumn;

  public TVListForwardIterator(ElasticSerializableTVList tvList) {
    this.tvList = tvList;
    // Point to dummy block
    externalIndex = 0;
    internalIndex = -1;
  }

  public TVListForwardIterator(
      ElasticSerializableTVList tvList, int externalIndex, int internalIndex) {
    this.tvList = tvList;
    this.externalIndex = externalIndex;
    this.internalIndex = internalIndex;
  }

  public TimeColumn currentTimes() throws IOException {
    return cachedTimeColumn == null
        ? tvList.getTimeColumn(externalIndex, internalIndex)
        : cachedTimeColumn;
  }

  public Column currentValues() throws IOException {
    return cachedValueColumn == null
        ? tvList.getValueColumn(externalIndex, internalIndex)
        : cachedValueColumn;
  }

  @Override
  public boolean hasNext() {
    // First time call, tvList has no data
    List<SerializableTVList> internalLists = tvList.getInternalTVList();
    if (internalLists.size() == 0) {
      return false;
    }

    return externalIndex + 1 < internalLists.size()
        || internalIndex + 1 < internalLists.get(externalIndex).getColumnCount();
  }

  @Override
  public void next() throws IOException {
    List<SerializableTVList> internalLists = tvList.getInternalTVList();
    if (internalIndex + 1 == internalLists.get(externalIndex).getColumnCount()) {
      internalIndex = 0;
      externalIndex++;
    } else {
      internalIndex++;
    }

    cachedTimeColumn = tvList.getTimeColumn(externalIndex, internalIndex);
    cachedValueColumn = tvList.getValueColumn(externalIndex, internalIndex);
  }

  // When tvList apply new memory control strategy, the origin tvList become invalid.
  // We must iterate all columns to find matched columns
  public void adjust() throws IOException {
    TVListForwardIterator iterator = tvList.constructIterator();

    while (iterator.hasNext()) {
      iterator.next();
      TimeColumn times = iterator.currentTimes();
      if (times == cachedTimeColumn) {
        this.externalIndex = iterator.externalIndex;
        this.internalIndex = iterator.internalIndex;
        break;
      }
    }
  }
}
