/*

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
   *
 * http://www.apache.org/licenses/LICENSE-2.0
    *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
   */

  package org.apache.iotdb.library.dmatch;

  import org.apache.iotdb.db.query.udf.api.UDTF;
  import org.apache.iotdb.db.query.udf.api.access.Row;
  import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
  import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
  import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
  import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
  import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
  import org.apache.iotdb.library.util.Util;
  import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
  
  /** This function calculates mae between two input series. */
  public class UDAFMae implements UDTF {
  
    private long count = 0;
    private double sum = 0.0;
  
    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
      validator
          .validateInputSeriesNumber(2)
          .validateInputSeriesDataType(
              0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
          .validateInputSeriesDataType(
              1, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
    }
  
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
        throws Exception {
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.DOUBLE);
      count = 0;
      sum = 0.0;
    }
  
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
      if (row.isNull(0) || row.isNull(1)) { // skip null rows
        return;
      }
      double y1 = Util.getValueAsDouble(row, 0);
      double y2 = Util.getValueAsDouble(row, 1);
      if (Double.isFinite(y1) && Double.isFinite(y2)) { // skip NaN rows
        count++;
        sum += Math.abs(y1 - y2);
      }
    }
  
    @Override
    public void terminate(PointCollector collector) throws Exception {
      if (count > 0) { // calculate mae only when there is more than 1 point
        double mae = sum / count;
        collector.putDouble(0, mae);
      } else {
        collector.putDouble(0, Double.NaN);
      }
    }
  }