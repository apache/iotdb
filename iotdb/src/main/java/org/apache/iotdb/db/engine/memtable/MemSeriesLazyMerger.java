/**
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
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;

public class MemSeriesLazyMerger implements TimeValuePairSorter {

  private List<TimeValuePairSorter> memSeriesList;

  public MemSeriesLazyMerger() {
    memSeriesList = new ArrayList<>();
  }

  /**
   * Constructor of MemSeriesLazyMerger.
   *
   * @param memSerieses Please ensure that the memSerieses are in ascending order by timestamp.
   */
  public MemSeriesLazyMerger(TimeValuePairSorter... memSerieses) {
    this();
    Collections.addAll(memSeriesList, memSerieses);
  }

  /**
   * IMPORTANT: Please ensure that the minimum timestamp of added {@link IWritableMemChunk} is
   * larger than any timestamps of the IWritableMemChunk already added in.
   */
  public void addMemSeries(TimeValuePairSorter series) {
    memSeriesList.add(series);
  }

  @Override
  public List<TimeValuePair> getSortedTimeValuePairList() {
    if (memSeriesList.size() == 0) {
      return new ArrayList<>();
    } else {
      List<TimeValuePair> ret = memSeriesList.get(0).getSortedTimeValuePairList();
      for (int i = 1; i < memSeriesList.size(); i++) {
        ret.addAll(memSeriesList.get(i).getSortedTimeValuePairList());
      }
      return ret;
    }
  }
}
