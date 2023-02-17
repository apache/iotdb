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

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * This function is used to calculate the distribution histogram of a single column of numerical
 * data.
 */
public class UDTFHistogram implements UDTF {

  private int[] bucket;
  private double gap;
  private int count;
  private double start;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            count -> (int) count > 0,
            "parameter $count$ should be larger than 0",
            validator.getParameters().getIntOrDefault("count", 1))
        .validate(
            params -> (double) params[0] <= (double) params[1],
            "parameter $end$ should be larger than or equal to $start$",
            validator.getParameters().getDoubleOrDefault("min", -Double.MAX_VALUE),
            validator.getParameters().getDoubleOrDefault("max", Double.MAX_VALUE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT32);
    // input param min is var start, param max is var end
    start = parameters.getDoubleOrDefault("min", -Double.MAX_VALUE);
    double end = parameters.getDoubleOrDefault("max", Double.MAX_VALUE);
    count = parameters.getIntOrDefault("count", 1);
    bucket = new int[count];
    gap = (end - start) / count;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double value = Util.getValueAsDouble(row);
    if (Double.isFinite(value)) {
      int id = Math.min(Math.max((int) Math.floor((value - start) / gap), 0), count - 1);
      bucket[id]++;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for (int i = 0; i < count; i++) {
      collector.putInt(i, bucket[i]);
    }
  }
}
