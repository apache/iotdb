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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.anomaly.util.StreamMissDetector;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function is used to detect missing anomalies. */
public class UDTFMissDetect implements UDTF {

  private StreamMissDetector detector;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            x -> (int) x >= 10,
            "minlen should be an integer greater than or equal to 10.",
            validator.getParameters().getIntOrDefault("minlen", 10));
  }

  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    udtfc.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.BOOLEAN);
    int minLength = udfp.getIntOrDefault("minlen", 10);
    this.detector = new StreamMissDetector(minLength);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    detector.insert(row.getTime(), Util.getValueAsDouble(row));
    while (detector.hasNext()) {
      collector.putBoolean(detector.getOutTime(), detector.getOutValue());
      detector.next();
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    detector.flush();
    while (detector.hasNext()) {
      collector.putBoolean(detector.getOutTime(), detector.getOutValue());
      detector.next();
    }
  }
}
