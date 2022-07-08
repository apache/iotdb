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

package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class TsBlockUtil {

  // skip points that cannot be calculated
  public static TsBlock skipToTimeRangePoints(
      TsBlock tsBlock, TimeRange targetTimeRange, boolean ascending) {
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? targetTimeRange.getMin() : targetTimeRange.getMax();
    int left = 0, right = timeColumn.getPositionCount() - 1, mid;
    // if ascending, find the first greater than or equal to targetTime
    // else, find the first less than or equal to targetTime
    while (left < right) {
      mid = (left + right) >> 1;
      if (timeColumn.getLongWithoutCheck(mid) < targetTime) {
        if (ascending) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) > targetTime) {
        if (ascending) {
          right = mid;
        } else {
          left = mid + 1;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) == targetTime) {
        return tsBlock.subTsBlock(mid);
      }
    }
    return tsBlock.subTsBlock(left);
  }

  public static TsBlock skipOutOfTimeRangePoints(
      TsBlock tsBlock, TimeRange curTimeRange, boolean ascending) {
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? curTimeRange.getMax() : curTimeRange.getMin();
    if (timeColumn.getPositionCount() == 1) {
      long checkedTime = timeColumn.getLongWithoutCheck(0);
      return tsBlock.subTsBlock(
          ((ascending && checkedTime <= targetTime) || (!ascending && checkedTime >= targetTime))
              ? 1
              : 0);
    }

    int left = 0, right = timeColumn.getPositionCount() - 1, mid;
    // if ascending, find the first greater than targetTime
    // else, find the first less than targetTime
    while (left < right) {
      mid = (left + right) >> 1;
      long checkedTime = timeColumn.getLongWithoutCheck(mid);
      if (ascending) {
        if (checkedTime <= targetTime) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else {
        if (checkedTime >= targetTime) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
    }
    return tsBlock.subTsBlock(left);
  }

  // check if the batchData does not contain points in current interval
  public static boolean satisfied(TsBlock tsBlock, TimeRange timeRange, boolean ascending) {
    TsBlock.TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
    if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
      return false;
    }

    return ascending
        ? (tsBlockIterator.getEndTime() >= timeRange.getMin()
            && tsBlockIterator.currentTime() <= timeRange.getMax())
        : (tsBlockIterator.getEndTime() <= timeRange.getMax()
            && tsBlockIterator.currentTime() >= timeRange.getMin());
  }
}
