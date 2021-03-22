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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.reader.series.BaseManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A placeholder when the remote node does not contain satisfying data of a series. */
public class EmptyReader extends BaseManagedSeriesReader
    implements ManagedSeriesReader,
        IAggregateReader,
        IPointReader,
        GroupByExecutor,
        IReaderByTimestamp {

  private List<AggregateResult> aggregationResults = new ArrayList<>();

  @Override
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    return null;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    return null;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public boolean hasNextFile() {
    return false;
  }

  @Override
  public boolean canUseCurrentFileStatistics() {
    return false;
  }

  @Override
  public Statistics currentFileStatistics() {
    return null;
  }

  @Override
  public void skipCurrentFile() {
    // do nothing
  }

  @Override
  public boolean hasNextChunk() {
    return false;
  }

  @Override
  public boolean canUseCurrentChunkStatistics() {
    return false;
  }

  @Override
  public Statistics currentChunkStatistics() {
    return null;
  }

  @Override
  public void skipCurrentChunk() {
    // do nothing
  }

  @Override
  public boolean hasNextPage() {
    return false;
  }

  @Override
  public boolean canUseCurrentPageStatistics() {
    return false;
  }

  @Override
  public Statistics currentPageStatistics() {
    return null;
  }

  @Override
  public void skipCurrentPage() {
    // do nothing
  }

  @Override
  public BatchData nextPage() {
    return null;
  }

  @Override
  public boolean isAscending() {
    return false;
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    aggregationResults.add(aggrResult);
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime) {
    return aggregationResults;
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps, int length) throws IOException {
    return null;
  }

  @Override
  public boolean readerIsEmpty() {
    return false;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime) {
    return null;
  }
}
