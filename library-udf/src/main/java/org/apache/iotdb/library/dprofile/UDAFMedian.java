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
import org.apache.iotdb.library.dprofile.util.ExactOrderStatistics;
import org.apache.iotdb.library.dprofile.util.GKArray;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;

/** calculate the exact or approximate median */
public class UDAFMedian implements UDTF {

  private ExactOrderStatistics statistics;
  private GKArray sketch;
  private boolean exact;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT32),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.INT64),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.FLOAT),
            UDFDataTypeTransformer.transformToUDFDataType(TSDataType.DOUBLE))
        .validate(
            error -> (double) error >= 0 && (double) error < 1,
            "error has to be greater than or equal to 0 and less than 1.",
            validator.getParameters().getDoubleOrDefault("error", 0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(TSDataType.DOUBLE));
    double error = parameters.getDoubleOrDefault("error", 0);
    exact = (error == 0);
    if (exact) {
      statistics =
          new ExactOrderStatistics(
              UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0)));
    } else {
      sketch = new GKArray(error);
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (exact) {
      statistics.insert(row);
    } else {
      sketch.insert(Util.getValueAsDouble(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (exact) {
      collector.putDouble(0, statistics.getMedian());
    } else {
      collector.putDouble(0, sketch.query(0.5));
    }
  }
}
