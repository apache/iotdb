/**
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
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.TimeValuePair;

public class MemSeriesLazyMerger implements TimeValuePairSorter {

  private List<ReadOnlyMemChunk> memSeriesList;

  public MemSeriesLazyMerger() {
    memSeriesList = new ArrayList<>();
  }

  /**
   * IMPORTANT: Please ensure that the minimum timestamp of added {@link IWritableMemChunk} is
   * larger than any timestamps of the IWritableMemChunk already added in.
   */
  public void addMemSeries(ReadOnlyMemChunk series) {
    memSeriesList.add(series);
  }

  @Override
  public List<TimeValuePair> getSortedTimeValuePairList() {
    List<TimeValuePair> res = new ArrayList<>();
    for (int i = 0; i < memSeriesList.size(); i++) {
      res.addAll(memSeriesList.get(i).getSortedTimeValuePairList());
    }
    return res;
  }
}
