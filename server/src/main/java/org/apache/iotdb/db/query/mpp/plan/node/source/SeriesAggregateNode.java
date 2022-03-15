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

package org.apache.iotdb.db.query.mpp.plan.node.source;

import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * SeriesAggregateOperator is responsible to do the aggregation calculation for one series. It will read the
 * target series and calculate the aggregation result by the aggregation digest or raw data of this series.
 *
 * The aggregation result will be represented as a TsBlock
 *
 * This operator will split data of the target series into many groups by time range and do the aggregation calculation
 * for each group. Each result will be one row of the result TsBlock. The timestamp of each row is the start time of the
 * time range group.
 *
 * If there is no time range split parameter, the result TsBlock will only contain one row, which represent the whole
 * aggregation result of this series. And the timestamp will be 0, which is meaningless.
 */
public class SeriesAggregateNode extends SourceNode {

    // The parameter of `group by time`
    // Its value will be null if there is no `group by time` clause,
    private GroupByTimeParameter groupByTimeParameter;

    // The aggregation function, which contains the function name and related series.
    // (Currently we only support one series in the aggregation function)
    // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
    private FunctionExpression aggregateFunc;

    private Filter filter;

    public SeriesAggregateNode(PlanNodeId id) {
        super(id);
    }

    public SeriesAggregateNode(PlanNodeId id, FunctionExpression aggregateFunc) {
        this(id);
        this.aggregateFunc = aggregateFunc;
    }

    public SeriesAggregateNode(PlanNodeId id, FunctionExpression aggregateFunc, GroupByTimeParameter groupByTimeParameter) {
        this(id, aggregateFunc);
        this.groupByTimeParameter = groupByTimeParameter;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    // This method is used when do the PredicatePushDown.
    // The filter is not put in the constructor because the filter is only clear in the predicate push-down stage
    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
