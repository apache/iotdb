/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class LastValueDescAccumulator extends LastValueAccumulator {

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  public void reset() {
    super.reset();
  }

  protected int addIntInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateIntLastValue(column[1].getInt(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }

  protected int addLongInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateLongLastValue(column[1].getLong(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }

  protected int addFloatInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateFloatLastValue(column[1].getFloat(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }

  protected int addDoubleInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateDoubleLastValue(column[1].getDouble(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }

  protected int addBooleanInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateBooleanLastValue(column[1].getBoolean(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }

  protected int addBinaryInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime > timeRange.getMax() || curTime < timeRange.getMin()) {
        return i;
      }
      if (!column[1].isNull(i)) {
        updateBinaryLastValue(column[1].getBinary(i), curTime);
        return i;
      }
    }
    return column[0].getPositionCount();
  }
}
