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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.exception.UnSupportedEventAggregationMeasureTypeException;
import org.apache.iotdb.db.mpp.execution.operator.exception.UnSupportedEventAggregationWindowTypeException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

public class WindowManagerFactory {

  public IWindowManager genWindowManager(
      WindowParameter windowParameter, ITimeRangeIterator timeRangeIterator) {
    switch (windowParameter.getWindowType()) {
      case TIME_WINDOW:
        return new TimeWindowManager(timeRangeIterator);
      case EVENT_WINDOW:
        return genEventWindowManager(windowParameter, timeRangeIterator.isAscending());
      default:
        throw new UnSupportedEventAggregationWindowTypeException(windowParameter.getWindowType());
    }
  }

  private EventWindowManager genEventWindowManager(
      WindowParameter windowParameter, boolean ascending) {
    switch (windowParameter.getCompareType()) {
      case EQUAL:
        return genEqualEventWindowManager(windowParameter, ascending);
      case VARIATION:
        return genVariationEventWindowManager(windowParameter, ascending);
      default:
        throw new UnSupportedEventAggregationMeasureTypeException(windowParameter.getCompareType());
    }
  }

  private EventWindowManager genEqualEventWindowManager(
      WindowParameter windowParameter, boolean ascending) {
    switch (windowParameter.getDataType()) {
      case INT32:
        return new EqualEventIntWindowManager(windowParameter, ascending);
      case INT64:
        return new EqualEventLongWindowManager(windowParameter, ascending);
      case FLOAT:
        return new EqualEventFloatWindowManager(windowParameter, ascending);
      case DOUBLE:
        return new EqualEventDoubleWindowManager(windowParameter, ascending);
      case TEXT:
        return new EqualEventTextWindowManager(windowParameter, ascending);
      case BOOLEAN:
        return new EqualEventBooleanWindowManager(windowParameter, ascending);
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in equal event aggregation : %s",
                windowParameter.getDataType()));
    }
  }

  private EventWindowManager genVariationEventWindowManager(
      WindowParameter windowParameter, boolean ascending) {
    switch (windowParameter.getDataType()) {
      case INT32:
        return new VariationEventIntWindowManager(windowParameter, ascending);
      case INT64:
        return new VariationEventLongWindowManager(windowParameter, ascending);
      case FLOAT:
        return new VariationEventFloatWindowManager(windowParameter, ascending);
      case DOUBLE:
        return new VariationEventDoubleWindowManager(windowParameter, ascending);
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in variation event aggregation : %s",
                windowParameter.getDataType()));
    }
  }
}
