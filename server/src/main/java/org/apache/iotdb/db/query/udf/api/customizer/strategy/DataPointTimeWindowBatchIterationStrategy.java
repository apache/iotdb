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

package org.apache.iotdb.db.query.udf.api.customizer.strategy;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.DatetimeUtils.DurationUnit;

public class DataPointTimeWindowBatchIterationStrategy extends DataPointBatchIterationStrategy {

  private final long timeInterval;
  private final long slidingStep;

  private Long displayWindowBegin;
  private Long displayWindowEnd;

  public DataPointTimeWindowBatchIterationStrategy(int seriesIndex,
      long displayWindowBegin, DurationUnit displayWindowBeginTimeUnit,
      long displayWindowEnd, DurationUnit displayWindowEndTimeUnit,
      long timeInterval, DurationUnit timeIntervalTimeUnit,
      long slidingStep, DurationUnit slidingStepTimeUnit) {
    super(seriesIndex);
    this.displayWindowBegin = DatetimeUtils
        .convertDurationStrToLong(displayWindowBegin, displayWindowBeginTimeUnit.toString(), "ns");
    this.displayWindowEnd = DatetimeUtils
        .convertDurationStrToLong(displayWindowEnd, displayWindowEndTimeUnit.toString(), "ns");
    this.timeInterval = DatetimeUtils
        .convertDurationStrToLong(timeInterval, timeIntervalTimeUnit.toString(), "ns");
    this.slidingStep = DatetimeUtils
        .convertDurationStrToLong(slidingStep, slidingStepTimeUnit.toString(), "ns");
  }

  public DataPointTimeWindowBatchIterationStrategy(int seriesIndex,
      long timeInterval, DurationUnit timeIntervalTimeUnit,
      long slidingStep, DurationUnit slidingStepTimeUnit) {
    super(seriesIndex);
    this.timeInterval = DatetimeUtils
        .convertDurationStrToLong(timeInterval, timeIntervalTimeUnit.toString(), "ns");
    this.slidingStep = DatetimeUtils
        .convertDurationStrToLong(slidingStep, slidingStepTimeUnit.toString(), "ns");
  }

  @Override
  public void check() throws QueryProcessException {
    super.check();
    if (timeInterval <= 0) {
      throw new QueryProcessException(
          String.format("Parameter timeInterval(%d) should be positive.", timeInterval));
    }
    if (slidingStep <= 0) {
      throw new QueryProcessException(
          String.format("Parameter slidingStep(%d) should be positive.", slidingStep));
    }
    if (hasDisplayWindowRange() && displayWindowEnd < displayWindowBegin) {
      throw new QueryProcessException(String.format("displayWindowEnd(%d) < displayWindowBegin(%d)",
          displayWindowEnd, displayWindowBegin));
    }
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  public long getSlidingStep() {
    return slidingStep;
  }

  public boolean hasDisplayWindowRange() {
    return displayWindowBegin != null && displayWindowEnd != null;
  }

  public Long getDisplayWindowBegin() {
    return displayWindowBegin;
  }

  public Long getDisplayWindowEnd() {
    return displayWindowEnd;
  }
}
