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

package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MergeGroupByExecutor implements GroupByExecutor {

  private static final Logger logger = LoggerFactory.getLogger(MergeGroupByExecutor.class);

  private List<AggregateResult> results = new ArrayList<>();
  private List<Integer> aggregationTypes = new ArrayList<>();
  private PartialPath path;
  private Set<String> deviceMeasurements;
  private TSDataType dataType;
  private QueryContext context;
  private Filter timeFilter;
  private ClusterReaderFactory readerFactory;
  private boolean ascending;

  private List<GroupByExecutor> groupByExecutors;

  MergeGroupByExecutor(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      MetaGroupMember metaGroupMember,
      boolean ascending) {
    this.path = path;
    this.deviceMeasurements = deviceMeasurements;
    this.dataType = dataType;
    this.context = context;
    this.timeFilter = timeFilter;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
    this.ascending = ascending;
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
    aggregationTypes.add(aggrResult.getAggregationType().ordinal());
  }

  private void resetAggregateResults() {
    for (AggregateResult result : results) {
      result.reset();
    }
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws QueryProcessException, IOException {
    if (groupByExecutors == null) {
      initExecutors();
    }
    resetAggregateResults();
    for (GroupByExecutor groupByExecutor : groupByExecutors) {
      List<AggregateResult> subResults = groupByExecutor.calcResult(curStartTime, curEndTime);
      for (int i = 0; i < subResults.size(); i++) {
        results.get(i).merge(subResults.get(i));
      }
    }
    logger.debug(
        "Aggregation result of {}@[{}, {}] is {}", path, curStartTime, curEndTime, results);
    return results;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    if (groupByExecutors == null) {
      try {
        initExecutors();
      } catch (QueryProcessException e) {
        throw new IOException(e);
      }
    }

    Pair<Long, Object> result = null;
    for (GroupByExecutor groupByExecutor : groupByExecutors) {
      Pair<Long, Object> pair = groupByExecutor.peekNextNotNullValue(nextStartTime, nextEndTime);
      if (pair == null) {
        continue;
      }
      if (result == null || result.left > pair.left) {
        result = pair;
      }
    }
    logger.debug(
        "peekNextNotNullValue result of {}@[{}, {}] is {}",
        path,
        nextStartTime,
        nextEndTime,
        results);
    return result;
  }

  private void initExecutors() throws QueryProcessException {
    try {
      groupByExecutors =
          readerFactory.getGroupByExecutors(
              path, deviceMeasurements, dataType, context, timeFilter, aggregationTypes, ascending);
    } catch (StorageEngineException e) {
      throw new QueryProcessException(e);
    }
  }
}
