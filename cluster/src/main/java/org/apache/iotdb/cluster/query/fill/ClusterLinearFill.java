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

package org.apache.iotdb.cluster.query.fill;

import org.apache.iotdb.cluster.query.aggregate.ClusterAggregator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import java.util.Arrays;
import java.util.List;

/**
 * ClusterLinearFill overrides the dataReader in LinearFill so that it can read data from the whole
 * cluster instead of only the local node.
 */
public class ClusterLinearFill extends LinearFill {

  private MetaGroupMember metaGroupMember;
  private ClusterAggregator aggregator;
  private static final List<String> AGGREGATION_NAMES =
      Arrays.asList(SQLConstant.MIN_TIME, SQLConstant.FIRST_VALUE);

  ClusterLinearFill(LinearFill fill, MetaGroupMember metaGroupMember) {
    super(fill.getDataType(), fill.getQueryTime(), fill.getBeforeRange(), fill.getAfterRange());
    this.metaGroupMember = metaGroupMember;
    this.aggregator = new ClusterAggregator(metaGroupMember);
  }

  @Override
  protected TimeValuePair calculatePrecedingPoint() {
    // calculate the preceding point can be viewed as a previous fill
    ClusterPreviousFill clusterPreviousFill =
        new ClusterPreviousFill(dataType, queryTime, beforeRange, metaGroupMember);
    clusterPreviousFill.configureFill(seriesPath, dataType, queryTime, deviceMeasurements, context);
    return clusterPreviousFill.getFillResult();
  }

  @Override
  protected TimeValuePair calculateSucceedingPoint() throws StorageEngineException {

    List<AggregateResult> aggregateResult =
        aggregator.getAggregateResult(
            seriesPath,
            deviceMeasurements,
            AGGREGATION_NAMES,
            dataType,
            afterFilter,
            context,
            true);
    AggregateResult minTimeResult = aggregateResult.get(0);
    AggregateResult firstValueResult = aggregateResult.get(1);

    return convertToResult(minTimeResult, firstValueResult);
  }
}
