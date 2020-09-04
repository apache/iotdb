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

package org.apache.iotdb.cluster.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
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

public class ClusterAggregateExecutor extends AggregationExecutor {

  private MetaGroupMember metaMember;

  /**
   * constructor.
   *
   * @param aggregationPlan
   */
  ClusterAggregateExecutor(AggregationPlan aggregationPlan, MetaGroupMember metaMember) {
    super(aggregationPlan);
    this.metaMember = metaMember;
  }

  @Override
  protected List<AggregateResult> aggregateOneSeries(Entry<PartialPath, List<Integer>> pathToAggrIndexes,
      Set<String> deviceMeasurements, Filter timeFilter, QueryContext context) throws StorageEngineException {
    PartialPath seriesPath = pathToAggrIndexes.getKey();
    TSDataType tsDataType = dataTypes.get(pathToAggrIndexes.getValue().get(0));
    List<String> aggregationNames = new ArrayList<>();

    for (int i : pathToAggrIndexes.getValue()) {
      aggregationNames.add(aggregations.get(i));
    }
    return metaMember.getAggregateResult(seriesPath, deviceMeasurements, aggregationNames,
        tsDataType, timeFilter,
        context);
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(expression, context, metaMember, rawDataQueryPlan);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(PartialPath path,
      RawDataQueryPlan dataQueryPlan, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return metaMember.getReaderByTimestamp(path, dataQueryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType, context);
  }
}
