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
package org.apache.iotdb.db.query.dataset.groupby;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public abstract class GroupByEngineDataSet extends QueryDataSet {

  protected long queryId;
  private long unit;
  private long slidingStep;
  private long intervalStartTime;
  private long intervalEndTime;

  protected long startTime;
  protected long endTime;
  private int usedIndex;
  protected List<AggregateResult> functions;
  protected boolean hasCachedTimeInterval;

  /**
   * groupBy query.
   */
  public GroupByEngineDataSet(QueryContext context, GroupByPlan groupByPlan) {
    super(groupByPlan.getDeduplicatedPaths(), groupByPlan.getDeduplicatedDataTypes());
    this.queryId = context.getQueryId();
    this.unit = groupByPlan.getUnit();
    this.slidingStep = groupByPlan.getSlidingStep();
    this.intervalStartTime = groupByPlan.getStartTime();
    this.intervalEndTime = groupByPlan.getEndTime();
    this.functions = new ArrayList<>();

    // init group by time partition
    this.usedIndex = 0;
    this.hasCachedTimeInterval = false;
    this.endTime = -1;
  }

  protected void initAggreFuction(GroupByPlan groupByPlan) throws PathException {
    // construct AggregateFunctions
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult function = AggreFuncFactory
          .getAggrFuncByName(groupByPlan.getDeduplicatedAggregations().get(i),
              groupByPlan.getDeduplicatedDataTypes().get(i));
      function.init();
      functions.add(function);
    }
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    startTime = usedIndex * slidingStep + intervalStartTime;
    usedIndex++;
    //This is an open interval , [0-100)
    if (startTime < intervalEndTime) {
      hasCachedTimeInterval = true;
      endTime = Math.min(startTime + unit, intervalEndTime + 1);
      return true;
    } else {
      return false;
    }
  }

  /**
   * this method is only used in the test class to get the next time partition.
   */
  public Pair<Long, Long> nextTimePartition() {
    hasCachedTimeInterval = false;
    return new Pair<>(startTime, endTime);
  }

  protected Field getField(AggreResultData aggreResultData) {
    if (!aggreResultData.isSetValue()) {
      return new Field(null);
    }
    Field field = new Field(aggreResultData.getDataType());
    switch (aggreResultData.getDataType()) {
      case INT32:
        field.setIntV(aggreResultData.getIntRet());
        break;
      case INT64:
        field.setLongV(aggreResultData.getLongRet());
        break;
      case FLOAT:
        field.setFloatV(aggreResultData.getFloatRet());
        break;
      case DOUBLE:
        field.setDoubleV(aggreResultData.getDoubleRet());
        break;
      case BOOLEAN:
        field.setBoolV(aggreResultData.isBooleanRet());
        break;
      case TEXT:
        field.setBinaryV(aggreResultData.getBinaryRet());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + aggreResultData.getDataType());
    }
    return field;
  }

}
