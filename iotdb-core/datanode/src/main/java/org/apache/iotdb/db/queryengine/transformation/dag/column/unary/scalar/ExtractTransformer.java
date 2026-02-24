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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.time.ZoneId;
import java.util.function.Function;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;
import static org.apache.iotdb.db.utils.DateTimeUtils.convertToZonedDateTime;
import static org.apache.iotdb.db.utils.DateTimeUtils.getExtractTimestampMsPartFunction;
import static org.apache.iotdb.db.utils.DateTimeUtils.getExtractTimestampNsPartFunction;
import static org.apache.iotdb.db.utils.DateTimeUtils.getExtractTimestampUsPartFunction;

public class ExtractTransformer extends UnaryColumnTransformer {
  private final Function<Long, Long> evaluateFunction;

  public ExtractTransformer(
      Type type, ColumnTransformer child, Extract.Field field, ZoneId zoneId) {
    super(type, child);
    this.evaluateFunction = constructEvaluateFunction(field, zoneId);
  }

  public static Function<Long, Long> constructEvaluateFunction(Extract.Field field, ZoneId zoneId) {
    switch (field) {
      case YEAR:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getYear();
      case QUARTER:
        return timestamp -> (convertToZonedDateTime(timestamp, zoneId).getMonthValue() + 2L) / 3L;
      case MONTH:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getMonthValue();
      case WEEK:
        return timestamp -> convertToZonedDateTime(timestamp, zoneId).getLong(ALIGNED_WEEK_OF_YEAR);
      case DAY:
      case DAY_OF_MONTH:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getDayOfMonth();
      case DAY_OF_WEEK:
      case DOW:
        return timestamp ->
            (long) convertToZonedDateTime(timestamp, zoneId).getDayOfWeek().getValue();
      case DAY_OF_YEAR:
      case DOY:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getDayOfYear();
      case HOUR:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getHour();
      case MINUTE:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getMinute();
      case SECOND:
        return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getSecond();
      case MS:
        return getExtractTimestampMsPartFunction();
      case US:
        return getExtractTimestampUsPartFunction();
      case NS:
        return getExtractTimestampNsPartFunction();
      default:
        throw new UnsupportedOperationException("Unexpected extract field: " + field);
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        columnBuilder.writeLong(evaluateFunction.apply(column.getLong(i)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        columnBuilder.writeLong(evaluateFunction.apply(column.getLong(i)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }
}
