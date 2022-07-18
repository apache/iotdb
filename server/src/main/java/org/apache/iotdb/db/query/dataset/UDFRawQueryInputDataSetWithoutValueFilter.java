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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.mpp.transformation.dag.input.IUDFInputDataSet;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;

import java.io.IOException;
import java.util.List;

public class UDFRawQueryInputDataSetWithoutValueFilter extends RawQueryDataSetWithoutValueFilter
    implements IUDFInputDataSet {

  public UDFRawQueryInputDataSetWithoutValueFilter(
      long queryId, UDTFPlan queryPlan, List<ManagedSeriesReader> readers)
      throws IOException, InterruptedException {
    super(queryId, queryPlan, readers);
  }

  @Override
  protected ReadTask generateReadTaskForGivenReader(ManagedSeriesReader reader, int seriesIndex) {
    return new ReadTask(
        reader, blockingQueueArray[seriesIndex], paths.get(seriesIndex).getFullPath(), null, 0, 0);
  }

  @Override
  public boolean hasNextRowInObjects() {
    return !timeHeap.isEmpty();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    int seriesNumber = seriesReaderList.size();

    long minTime = timeHeap.pollFirst();
    Object[] rowInObjects = new Object[seriesNumber + 1];
    rowInObjects[seriesNumber] = minTime;

    for (int seriesIndex = 0; seriesIndex < seriesNumber; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] != null
          && cachedBatchDataArray[seriesIndex].hasCurrent()
          && cachedBatchDataArray[seriesIndex].currentTime() == minTime) {
        rowInObjects[seriesIndex] = cachedBatchDataArray[seriesIndex].currentValue();
        super.cacheNext(seriesIndex);
      }
    }

    return rowInObjects;
  }
}
