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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public class GroupByFillDataSet extends QueryDataSet {

  private GroupByEngineDataSet groupByEngineDataSet;
  private Map<TSDataType, IFill> fillTypes;
  // the first value for each time series
  private Object[] previousValue;
  private long[] previousTime;
  // last timestamp for each time series
  private long[] lastTimeArray;
  private TimeValuePair[] firstNotNullTV;
  private boolean isPeekEnded = false;

  public GroupByFillDataSet(List<PartialPath> paths, List<TSDataType> dataTypes,
      GroupByEngineDataSet groupByEngineDataSet,
      Map<TSDataType, IFill> fillTypes, QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    super(new ArrayList<>(paths), dataTypes, groupByFillPlan.isAscending());
    this.groupByEngineDataSet = groupByEngineDataSet;
    this.fillTypes = fillTypes;
    initPreviousParis(context, groupByFillPlan);
    initLastTimeArray(context, groupByFillPlan);
  }

  private void initPreviousParis(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    previousValue = new Object[paths.size()];
    previousTime = new long[paths.size()];
    firstNotNullTV = new TimeValuePair[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = (PartialPath) paths.get(i);
      TSDataType dataType = dataTypes.get(i);
      IFill fill;
      if (fillTypes.containsKey(dataType)) {
        fill = new PreviousFill(dataType, groupByEngineDataSet.getStartTime(),
            ((PreviousFill) fillTypes.get(dataType)).getBeforeRange(),
            ((PreviousFill) fillTypes.get(dataType)).isUntilLast());
      } else {
        fill = new PreviousFill(dataType, groupByEngineDataSet.getStartTime(),
            IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval());
      }
      fill.configureFill(path, dataType, groupByEngineDataSet.getStartTime(),
          groupByFillPlan.getAllMeasurementsInDevice(path.getDevice()), context);

      firstNotNullTV[i] = fill.getFillResult();
      TimeValuePair timeValuePair = firstNotNullTV[i];
      previousValue[i] = null;
      previousTime[i] = Long.MAX_VALUE;
      if (ascending && timeValuePair != null && timeValuePair.getValue() != null) {
        previousValue[i] = timeValuePair.getValue().getValue();
        previousTime[i] = timeValuePair.getTimestamp();
      }
    }
  }

  private void initLastTimeArray(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws IOException, StorageEngineException, QueryProcessException {
    lastTimeArray = new long[paths.size()];
    Arrays.fill(lastTimeArray, Long.MAX_VALUE);
    for (int i = 0; i < paths.size(); i++) {
      TimeValuePair lastTimeValuePair;
      lastTimeValuePair = LastQueryExecutor.calculateLastPairForOneSeriesLocally(
          (PartialPath) paths.get(i), dataTypes.get(i), context,
          groupByFillPlan.getAllMeasurementsInDevice(paths.get(i).getDevice()));
      if (lastTimeValuePair.getValue() != null) {
        lastTimeArray[i] = lastTimeValuePair.getTimestamp();
      }
    }
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    return groupByEngineDataSet.hasNextWithoutConstraint();
  }

  @Override
  @SuppressWarnings("squid:S3776")
  protected RowRecord nextWithoutConstraint() throws IOException {
    RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();

    for (int i = 0; i < paths.size(); i++) {
      Field field = rowRecord.getFields().get(i);
      // current group by result is null
      if (field == null || field.getDataType() == null) {
        // the previous value is not null
        // and (fill type is not previous until last or now time is before last time)
        // and (previous before range is not limited or previous before range contains the previous interval)
        TSDataType tsDataType = dataTypes.get(i);
        PreviousFill previousFill = (PreviousFill) fillTypes.get(tsDataType);
        if (previousValue[i] != null
            && ((fillTypes.containsKey(tsDataType) && !previousFill.isUntilLast())
            || rowRecord.getTimestamp() <= lastTimeArray[i])
            && (!fillTypes.containsKey(tsDataType) || previousFill.getBeforeRange() < 0
            || previousFill.getBeforeRange() >= groupByEngineDataSet.interval)
            && rowRecord.getTimestamp() >= previousTime[i]) {
          rowRecord.getFields().set(i, Field.getField(previousValue[i], tsDataType));
        } else if (!ascending && !isPeekEnded) {
          Pair<Long, Object> data = groupByEngineDataSet.peekNextNotNullValue(paths.get(i), i);
          if (data == null) {
            isPeekEnded = true;
            previousTime[i] = Long.MIN_VALUE;
            previousValue[i] = null;
            if (firstNotNullTV[i] != null && firstNotNullTV[i].getValue() != null) {
              rowRecord.getFields().set(i,
                  Field.getField(firstNotNullTV[i].getValue().getValue(), tsDataType));
              previousValue[i] = firstNotNullTV[i].getValue().getValue();
              previousTime[i] = firstNotNullTV[i].getTimestamp();
            }
            //data != null
          } else {
            rowRecord.getFields().set(i, Field.getField(data.right, tsDataType));
            previousValue[i] = data.right;
            previousTime[i] = data.left;
          }
        }
      } else {
        // use now value update previous value
        previousValue[i] = field.getObjectValue(field.getDataType());
        previousTime[i] = rowRecord.getTimestamp();
      }
    }
    return rowRecord;
  }
}
