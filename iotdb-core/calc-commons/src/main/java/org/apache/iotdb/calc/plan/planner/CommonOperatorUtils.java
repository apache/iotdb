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

package org.apache.iotdb.calc.plan.planner;

import org.apache.iotdb.calc.execution.operator.process.fill.IFill;
import org.apache.iotdb.calc.execution.operator.process.fill.IFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.filter.FixedIntervalFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.filter.MonthIntervalMSFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.filter.MonthIntervalNSFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.filter.MonthIntervalUSFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.identity.IdentityFill;
import org.apache.iotdb.calc.execution.operator.process.fill.identity.IdentityLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.DoubleLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.FloatLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.IntLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.linear.LongLinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BinaryPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BinaryPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BooleanPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.BooleanPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.DoublePreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.DoublePreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.FloatPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.FloatPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.IntPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.IntPreviousFillWithTimeDuration;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.LongPreviousFill;
import org.apache.iotdb.calc.execution.operator.process.fill.previous.LongPreviousFillWithTimeDuration;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.TimeDuration;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils.TIMESTAMP_PRECISION;

public class CommonOperatorUtils {
  public static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();
  public static final String UNKNOWN_DATATYPE = "Unknown data type: ";
  public static final String CURRENT_DEVICE_INDEX_STRING = "CurrentDeviceIndex";
  public static final LongColumn TIME_COLUMN_TEMPLATE =
      new LongColumn(1, Optional.empty(), new long[] {0});
  public static final String CURRENT_USED_MEMORY = "CurrentUsedMemory";
  public static final String MAX_USED_MEMORY = "MaxUsedMemory";
  public static final String MAX_RESERVED_MEMORY = "MaxReservedMemory";
  public static final IdentityFill IDENTITY_FILL = new IdentityFill();

  public static ILinearFill[] getLinearFill(int inputColumns, List<TSDataType> inputDataTypes) {
    ILinearFill[] linearFill = new ILinearFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case INT32:
        case DATE:
          linearFill[i] = new IntLinearFill();
          break;
        case INT64:
        case TIMESTAMP:
          linearFill[i] = new LongLinearFill();
          break;
        case FLOAT:
          linearFill[i] = new FloatLinearFill();
          break;
        case DOUBLE:
          linearFill[i] = new DoubleLinearFill();
          break;
        case BOOLEAN:
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          linearFill[i] = IDENTITY_LINEAR_FILL;
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return linearFill;
  }

  public static IFill[] getPreviousFill(
      int inputColumns,
      List<TSDataType> inputDataTypes,
      TimeDuration timeDurationThreshold,
      ZoneId zoneId) {
    IFillFilter filter;
    if (timeDurationThreshold == null) {
      filter = null;
    } else if (!timeDurationThreshold.containsMonth()) {
      filter = new FixedIntervalFillFilter(timeDurationThreshold.nonMonthDuration);
    } else {
      switch (TIMESTAMP_PRECISION) {
        case "ms":
          filter =
              new MonthIntervalMSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        case "us":
          filter =
              new MonthIntervalUSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        case "ns":
          filter =
              new MonthIntervalNSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        default:
          // this case will never reach
          throw new UnsupportedOperationException(
              "not supported time_precision: " + TIMESTAMP_PRECISION);
      }
    }

    IFill[] previousFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          previousFill[i] =
              filter == null
                  ? new BooleanPreviousFill()
                  : new BooleanPreviousFillWithTimeDuration(filter);
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          previousFill[i] =
              filter == null
                  ? new BinaryPreviousFill()
                  : new BinaryPreviousFillWithTimeDuration(filter);
          break;
        case INT32:
        case DATE:
          previousFill[i] =
              filter == null ? new IntPreviousFill() : new IntPreviousFillWithTimeDuration(filter);
          break;
        case INT64:
        case TIMESTAMP:
          previousFill[i] =
              filter == null
                  ? new LongPreviousFill()
                  : new LongPreviousFillWithTimeDuration(filter);
          break;
        case FLOAT:
          previousFill[i] =
              filter == null
                  ? new FloatPreviousFill()
                  : new FloatPreviousFillWithTimeDuration(filter);
          break;
        case DOUBLE:
          previousFill[i] =
              filter == null
                  ? new DoublePreviousFill()
                  : new DoublePreviousFillWithTimeDuration(filter);
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return previousFill;
  }
}
