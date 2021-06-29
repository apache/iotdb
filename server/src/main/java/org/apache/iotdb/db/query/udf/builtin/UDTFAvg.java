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

package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class UDTFAvg implements UDTF {

  protected int pathsNum;
  protected TSDataType dataType;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    pathsNum = parameters.getPaths().size();
    dataType = parameters.getDataType(0);
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(dataType);
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws UDFInputSeriesDataTypeNotValidException, IOException {
    long time = row.getTime();
    switch (dataType) {
      case INT32:
        {
          int result = 0;
          for (int i = 0; i < pathsNum; i++) {
            try {
              result += row.getInt(i);
            } catch (NullPointerException e) {
              continue;
            }
          }
          collector.putInt(time, result / pathsNum);
        }
        break;
      case INT64:
        {
          long result = 0;
          for (int i = 0; i < pathsNum; i++) {
            try {
              result += row.getLong(i);
            } catch (NullPointerException e) {
              continue;
            }
          }
          collector.putLong(time, result / pathsNum);
        }
        break;
      case FLOAT:
        {
          float result = 0;
          for (int i = 0; i < pathsNum; i++) {
            try {
              result += row.getFloat(i);
            } catch (NullPointerException e) {
              continue;
            }
          }
          collector.putFloat(time, result / pathsNum);
        }
        break;
      case DOUBLE:
        {
          double result = 0;
          for (int i = 0; i < pathsNum; i++) {
            try {
              result += row.getDouble(i);
            } catch (NullPointerException e) {
              continue;
            }
          }
          collector.putDouble(time, result / pathsNum);
        }
        break;
      default:
        // This will not happen.
        throw new UDFInputSeriesDataTypeNotValidException(
            0, dataType, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  }

  protected void setTransformer() {
    throw new UnsupportedOperationException("UDTFAvg#setTransformer()");
  }
}
