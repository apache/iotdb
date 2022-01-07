/*
 * Copyright © 2021 iotdb-quality developer group (iotdb-quality@protonmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.quality.anomaly;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.quality.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/** This function is used to detect range anomaly of time series. */
public class UDTFRange implements UDTF {
  private TSDataType dataType;
  private double upperBound;
  private double lowerBound;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    this.lowerBound = parameters.getDouble("lower_bound");
    this.upperBound = parameters.getDouble("upper_bound");
    this.dataType = parameters.getDataType(0);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    long timestamp;
    timestamp = row.getTime();
    switch (dataType) {
      case INT32:
        intValue = row.getInt(0);
        if (intValue > upperBound || intValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, intValue);
        }
        break;
      case INT64:
        longValue = row.getLong(0);
        if (longValue > upperBound || longValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, longValue);
        }
        break;
      case FLOAT:
        floatValue = row.getFloat(0);
        if (floatValue > upperBound || floatValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, floatValue);
        }
        break;
      case DOUBLE:
        doubleValue = row.getDouble(0);
        if (doubleValue > upperBound || doubleValue < lowerBound) {
          Util.putValue(collector, dataType, timestamp, doubleValue);
        }
        break;
      default:
        throw new Exception();
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {}
}
