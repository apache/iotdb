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

package org.apache.iotdb.library.dmatch;

import org.apache.iotdb.library.dmatch.util.CrossCorrelation;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

/** This function calculates cross correlation between two input series. */
public class UDTFXCorr implements UDTF {

  private final ArrayList<Double> valueArrayList0 = new ArrayList<>();
  private final ArrayList<Double> valueArrayList1 = new ArrayList<>();

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    udtfConfigurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(Type.DOUBLE);
    valueArrayList0.clear();
    valueArrayList1.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) {
      valueArrayList0.add(Double.NaN);
      valueArrayList1.add(Double.NaN);
    } else {
      valueArrayList0.add(Util.getValueAsDouble(row, 0));
      valueArrayList1.add(Util.getValueAsDouble(row, 1));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    ArrayList<Double> correlationArrayList =
        CrossCorrelation.calculateCrossCorrelation(valueArrayList0, valueArrayList1);
    for (int i = 0; i < correlationArrayList.size(); i++) {
      collector.putDouble(i, correlationArrayList.get(i));
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validateInputSeriesDataType(1, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeDestroy() {
    valueArrayList0.clear();
    valueArrayList1.clear();
  }
}
