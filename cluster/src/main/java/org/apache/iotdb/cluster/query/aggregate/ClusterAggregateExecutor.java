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

package org.apache.iotdb.cluster.query.aggregate;

import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClusterAggregateExecutor extends AggregationExecutor {

  private MetaGroupMember metaMember;
  private ClusterReaderFactory readerFactory;
  private ClusterAggregator aggregator;

  /**
   * constructor.
   *
   * @param aggregationPlan
   */
  public ClusterAggregateExecutor(
      QueryContext context, AggregationPlan aggregationPlan, MetaGroupMember metaMember) {
    super(context, aggregationPlan);
    this.metaMember = metaMember;
    this.readerFactory = new ClusterReaderFactory(metaMember);
    this.aggregator = new ClusterAggregator(metaMember);
  }

  @Override
  protected void aggregateOneSeries(
      PartialPath seriesPath,
      List<Integer> indexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws StorageEngineException {
    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    List<String> aggregationNames = new ArrayList<>();

    for (int i : indexes) {
      aggregationNames.add(aggregations.get(i));
    }
    List<AggregateResult> aggregateResult =
        aggregator.getAggregateResult(
            seriesPath,
            allMeasurementsInDevice,
            aggregationNames,
            tsDataType,
            timeFilter,
            context,
            ascending);
    int rstIndex = 0;
    for (int i : indexes) {
      aggregateResultList[i] = aggregateResult.get(rstIndex++);
    }
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(context, metaMember, rawDataQueryPlan, false);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan dataQueryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(
        path,
        dataQueryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        dataQueryPlan.isAscending(),
        null);
  }
}
