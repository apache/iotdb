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
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.i18n.QueryMessages;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.TimeDuration;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils.TIMESTAMP_PRECISION;

public class CommonOperatorUtils {
  public static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();
  public static final String UNKNOWN_DATATYPE = CalcMessages.UNKNOWN_DATA_TYPE;
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
      linearFill[i] =
          TypeServices.LINEAR_FILL_SERVICE.call(Type.fromTsDataType(inputDataTypes.get(i))).get();
    }
    return linearFill;
  }

  public static IFill[] getPreviousFill(
      int inputColumns,
      List<TSDataType> inputDataTypes,
      TimeDuration timeDurationThreshold,
      ZoneId zoneId) {
    IFillFilter filter = createFillFilter(timeDurationThreshold, zoneId);

    IFill[] previousFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      previousFill[i] =
          TypeServices.PREVIOUS_FILL_SERVICE
              .call(Type.fromTsDataType(inputDataTypes.get(i)))
              .apply(filter);
    }
    return previousFill;
  }

  public static ILinearFill[] getNextFill(
      int inputColumns,
      List<TSDataType> inputDataTypes,
      TimeDuration timeDurationThreshold,
      ZoneId zoneId) {
    IFillFilter filter = createFillFilter(timeDurationThreshold, zoneId);

    ILinearFill[] nextFill = new ILinearFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      nextFill[i] =
          TypeServices.NEXT_FILL_SERVICE
              .call(Type.fromTsDataType(inputDataTypes.get(i)))
              .apply(filter);
    }
    return nextFill;
  }

  private static IFillFilter createFillFilter(TimeDuration timeDurationThreshold, ZoneId zoneId) {
    if (timeDurationThreshold == null) {
      return null;
    }
    if (!timeDurationThreshold.containsMonth()) {
      return new FixedIntervalFillFilter(timeDurationThreshold.nonMonthDuration);
    }
    switch (TIMESTAMP_PRECISION) {
      case "ms":
        return new MonthIntervalMSFillFilter(
            timeDurationThreshold.monthDuration, timeDurationThreshold.nonMonthDuration, zoneId);
      case "us":
        return new MonthIntervalUSFillFilter(
            timeDurationThreshold.monthDuration, timeDurationThreshold.nonMonthDuration, zoneId);
      case "ns":
        return new MonthIntervalNSFillFilter(
            timeDurationThreshold.monthDuration, timeDurationThreshold.nonMonthDuration, zoneId);
      default:
        // this case will never reach
        throw new UnsupportedOperationException(
            String.format(QueryMessages.UNSUPPORTED_TIME_PRECISION, TIMESTAMP_PRECISION));
    }
  }
}
