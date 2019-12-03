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
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public abstract class GroupByEngineDataSet extends QueryDataSet {

  protected long jobId;
  protected List<Path> selectedSeries;
  private long unit;
  private long origin;
  private List<Pair<Long, Long>> mergedIntervals;

  protected long startTime;
  protected long endTime;
  private int usedIndex;
  protected List<AggregateFunction> functions;
  protected boolean hasCachedTimeInterval;

  /**
   * groupBy query.
   */
  public GroupByEngineDataSet(long jobId, List<Path> paths, long unit, long origin,
      List<Pair<Long, Long>> mergedIntervals) {
    super(paths);
    this.jobId = jobId;
    this.selectedSeries = paths;
    this.unit = unit;
    this.origin = origin;
    this.mergedIntervals = mergedIntervals;
    this.functions = new ArrayList<>();

    // init group by time partition
    this.usedIndex = 0;
    this.hasCachedTimeInterval = false;
    this.endTime = -1;
  }

  protected void initAggreFuction(List<String> aggres) throws MetadataException {
    List<TSDataType> types = new ArrayList<>();
    // construct AggregateFunctions
    for (int i = 0; i < paths.size(); i++) {
      TSDataType tsDataType = MManager.getInstance()
          .getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
      function.init();
      functions.add(function);
      types.add(function.getResultDataType());
    }
    super.setDataTypes(types);
  }

  @Override
  public boolean hasNext() {
    // has cached
    if (hasCachedTimeInterval) {
      return true;
    }

    // skip the intervals in coverage of last time-partition
    while (usedIndex < mergedIntervals.size() && mergedIntervals.get(usedIndex).right < endTime) {
      usedIndex++;
    }

    // end
    if (usedIndex >= mergedIntervals.size()) {
      return false;
    }

    // initialize the start-end time of next interval
    if (endTime < mergedIntervals.get(usedIndex).left) {
      // interval start time
      startTime = mergedIntervals.get(usedIndex).left;
      if (origin > startTime) {
        endTime = origin - (origin - startTime) / unit * unit;
        if (endTime == startTime) { // origin - startTime is an integral multiple of unit
          endTime += unit;
        }
      } else {
        endTime = origin + (startTime - origin) / unit * unit + unit;
      }
      hasCachedTimeInterval = true;
      return true;
    }

    // current interval is not covered yet
    if (endTime <= mergedIntervals.get(usedIndex).right) {
      startTime = endTime;
      endTime += unit;
      hasCachedTimeInterval = true;
      return true;
    }
    return false;
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
