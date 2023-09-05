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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public abstract class UDTFContinuouslySatisfy implements UDTF {
  protected long min;
  protected long max;
  protected TSDataType dataType;
  protected long satisfyValueCount;
  protected long satisfyValueLastTime;
  protected long satisfyValueStartTime;
  protected Pair<Long, Long> interval;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN)
        .validate(
            args -> (Long) args[1] >= (Long) args[0],
            "max can not be smaller than min.",
            validator.getParameters().getLongOrDefault("min", getDefaultMin()),
            validator.getParameters().getLongOrDefault("max", getDefaultMax()))
        .validate(
            min -> (Long) min >= getDefaultMin(),
            "min can not be smaller than " + getDefaultMin() + ".",
            validator.getParameters().getLongOrDefault("min", getDefaultMin()));
  }

  protected abstract long getDefaultMin();

  protected abstract long getDefaultMax();

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException, UDFInputSeriesDataTypeNotValidException {
    satisfyValueCount = 0L;
    satisfyValueStartTime = 0L;
    satisfyValueLastTime = -1L;

    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    min = parameters.getLongOrDefault("min", getDefaultMin());
    max = parameters.getLongOrDefault("max", getDefaultMax());
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT64);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    boolean needAddNewRecord;
    switch (dataType) {
      case INT32:
        needAddNewRecord = transformInt(row.getTime(), row.getInt(0));
        break;
      case INT64:
        needAddNewRecord = transformLong(row.getTime(), row.getLong(0));
        break;
      case FLOAT:
        needAddNewRecord = transformFloat(row.getTime(), row.getFloat(0));
        break;
      case DOUBLE:
        needAddNewRecord = transformDouble(row.getTime(), row.getDouble(0));
        break;
      case BOOLEAN:
        needAddNewRecord = transformBoolean(row.getTime(), row.getBoolean(0));
        break;
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
    if (needAddNewRecord) {
      collector.putLong(interval.left, interval.right);
    }
  }

  protected boolean transformDouble(long time, double value) {
    if (satisfyDouble(value)) {
      if (satisfyValueCount == 0L) {
        satisfyValueStartTime = time;
      }
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
      interval = new Pair<>(satisfyValueStartTime, getRecord());
      satisfyValueCount = 0L;
      return true;
    } else {
      satisfyValueCount = 0L;
    }
    return false;
  }

  protected boolean transformFloat(long time, float value) {
    if (satisfyFloat(value)) {
      if (satisfyValueCount == 0L) {
        satisfyValueStartTime = time;
      }
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
      interval = new Pair<>(satisfyValueStartTime, getRecord());
      satisfyValueCount = 0L;
      return true;
    } else {
      satisfyValueCount = 0L;
    }
    return false;
  }

  protected boolean transformLong(long time, long value) {
    if (satisfyLong(value)) {
      if (satisfyValueCount == 0L) {
        satisfyValueStartTime = time;
      }
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
      interval = new Pair<>(satisfyValueStartTime, getRecord());
      satisfyValueCount = 0L;
      return true;
    } else {
      satisfyValueCount = 0L;
    }
    return false;
  }

  protected boolean transformInt(long time, int value) {
    if (satisfyInt(value)) {
      if (satisfyValueCount == 0L) {
        satisfyValueStartTime = time;
      }
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
      interval = new Pair<>(satisfyValueStartTime, getRecord());
      satisfyValueCount = 0L;
      return true;
    } else {
      satisfyValueCount = 0L;
    }
    return false;
  }

  protected boolean transformBoolean(long time, boolean value) {
    if (satisfyBoolean(value)) {
      if (satisfyValueCount == 0L) {
        satisfyValueStartTime = time;
      }
      satisfyValueCount++;
      satisfyValueLastTime = time;
    } else if (getRecord() >= min && getRecord() <= max && satisfyValueCount > 0) {
      interval = new Pair<>(satisfyValueStartTime, getRecord());
      satisfyValueCount = 0L;
      return true;
    } else {
      satisfyValueCount = 0L;
    }
    return false;
  }

  // To define the value you want to calculate.
  // Return `satisfyValueCount`, for the number of data points in the interval
  // Return `satisfyValueLastTime`, to get the interval start time and end time pair
  // Return `satisfyValueLastTime - satisfyValueStartTime` for the interval duration
  protected abstract Long getRecord();

  @Override
  public void terminate(PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    switch (dataType) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        if (satisfyValueCount > 0) {
          if (getRecord() >= min && getRecord() <= max) {
            collector.putLong(satisfyValueStartTime, getRecord());
          }
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  protected abstract boolean satisfyInt(int value);

  protected abstract boolean satisfyLong(long value);

  protected abstract boolean satisfyFloat(float value);

  protected abstract boolean satisfyDouble(double value);

  protected abstract boolean satisfyBoolean(Boolean value);
}
