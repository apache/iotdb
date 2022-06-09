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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;

/** This function is used to calculate the population standard deviation. */
public class UDAFStddev implements UDTF {
  private long count = 0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT32),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT64),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.FLOAT),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.DOUBLE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(TSDataType.DOUBLE));
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      this.count++;
      this.sumX1 += value;
      this.sumX2 += value * value;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    collector.putDouble(
        0, Math.sqrt(this.sumX2 / this.count - Math.pow(this.sumX1 / this.count, 2)));
  }
}
