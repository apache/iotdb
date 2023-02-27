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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

public class WindowManagerFactory {

  public static IWindowManager genWindowManager(
      WindowParameter windowParameter, ITimeRangeIterator timeRangeIterator, boolean ascending) {
    switch (windowParameter.getWindowType()) {
      case TIME_WINDOW:
        return new TimeWindowManager(timeRangeIterator, (TimeWindowParameter) windowParameter);
      case VARIATION_WINDOW:
        return ((VariationWindowParameter) windowParameter).getDelta() == 0
            ? genEqualEventWindowManager((VariationWindowParameter) windowParameter, ascending)
            : genVariationEventWindowManager((VariationWindowParameter) windowParameter, ascending);
      case CONDITION_WINDOW:
        return new ConditionWindowManager((ConditionWindowParameter) windowParameter);
      case SESSION_WINDOW:
        return new SessionWindowManager(
            windowParameter.isNeedOutputEndTime(),
            ((SessionWindowParameter) windowParameter).getTimeInterval(),
            ascending);
      default:
        throw new IllegalArgumentException(
            "Not support this type of aggregation window :"
                + windowParameter.getWindowType().name());
    }
  }

  private static VariationWindowManager genEqualEventWindowManager(
      VariationWindowParameter eventWindowParameter, boolean ascending) {
    switch (eventWindowParameter.getDataType()) {
      case INT32:
        return new EqualIntWindowManager(eventWindowParameter, ascending);
      case INT64:
        return new EqualLongWindowManager(eventWindowParameter, ascending);
      case FLOAT:
        return new EqualFloatWindowManager(eventWindowParameter, ascending);
      case DOUBLE:
        return new EqualDoubleWindowManager(eventWindowParameter, ascending);
      case TEXT:
        return new EqualBinaryWindowManager(eventWindowParameter, ascending);
      case BOOLEAN:
        return new EqualBooleanWindowManager(eventWindowParameter, ascending);
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in equal event aggregation : %s",
                eventWindowParameter.getDataType()));
    }
  }

  private static VariationWindowManager genVariationEventWindowManager(
      VariationWindowParameter eventWindowParameter, boolean ascending) {
    switch (eventWindowParameter.getDataType()) {
      case INT32:
        return new VariationIntWindowManager(eventWindowParameter, ascending);
      case INT64:
        return new VariationLongWindowManager(eventWindowParameter, ascending);
      case FLOAT:
        return new VariationFloatWindowManager(eventWindowParameter, ascending);
      case DOUBLE:
        return new VariationDoubleWindowManager(eventWindowParameter, ascending);
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in variation event aggregation : %s",
                eventWindowParameter.getDataType()));
    }
  }
}
