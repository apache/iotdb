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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * Return a series that the consecutive identical values in input series are removed (keeping only
 * the first one).
 */
public class UDTFChangePoints implements UDTF {
  private boolean isFirst = true;
  private Type dataType;
  private boolean cacheBoolean;
  private int cacheInt;
  private long cacheLong;
  private float cacheFloat;
  private double cacheDouble;
  private String cacheString;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(1);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    dataType = parameters.getDataType(0);
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(dataType);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    switch (dataType) {
      case BOOLEAN:
        if (isFirst) {
          isFirst = false;
          cacheBoolean = row.getBoolean(0);
          collector.putBoolean(row.getTime(), cacheBoolean);
        } else {
          boolean rowData = row.getBoolean(0);
          if (rowData != cacheBoolean) {
            cacheBoolean = rowData;
            collector.putBoolean(row.getTime(), cacheBoolean);
          }
        }
        break;
      case INT32:
        if (isFirst) {
          isFirst = false;
          cacheInt = row.getInt(0);
          collector.putInt(row.getTime(), cacheInt);
        } else {
          int rowData = row.getInt(0);
          if (rowData != cacheInt) {
            cacheInt = rowData;
            collector.putInt(row.getTime(), cacheInt);
          }
        }
        break;
      case INT64:
        if (isFirst) {
          isFirst = false;
          cacheLong = row.getLong(0);
          collector.putLong(row.getTime(), cacheLong);
        } else {
          long rowData = row.getLong(0);
          if (rowData != cacheLong) {
            cacheLong = rowData;
            collector.putLong(row.getTime(), cacheLong);
          }
        }
        break;
      case FLOAT:
        if (isFirst) {
          isFirst = false;
          cacheFloat = row.getFloat(0);
          collector.putFloat(row.getTime(), cacheFloat);
        } else {
          float rowData = row.getFloat(0);
          if (rowData != cacheFloat) {
            cacheFloat = rowData;
            collector.putFloat(row.getTime(), cacheFloat);
          }
        }
        break;
      case DOUBLE:
        if (isFirst) {
          isFirst = false;
          cacheDouble = row.getDouble(0);
          collector.putDouble(row.getTime(), cacheDouble);
        } else {
          double rowData = row.getDouble(0);
          if (rowData != cacheDouble) {
            cacheDouble = rowData;
            collector.putDouble(row.getTime(), cacheDouble);
          }
        }
        break;
      case TEXT:
        if (isFirst) {
          isFirst = false;
          cacheString = row.getString(0);
          collector.putString(row.getTime(), cacheString);
        } else {
          String rowData = row.getString(0);
          if (!rowData.equals(cacheString)) {
            cacheString = rowData;
            collector.putString(row.getTime(), cacheString);
          }
        }
    }
  }
}
