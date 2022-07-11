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
package org.apache.iotdb.commons.udf.builtin.String;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/*This function return a substring from target string, starting at position start and ending at position end - 1.
If parameter "end" is not existed or more than length of target, return the substring from start to end of target.*/
public class UDTFSubstr implements UDTF {

  int start;
  int end;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    int start = validator.getParameters().getInt("start");
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.TEXT)
        .validate(
            startPosition -> ((int) startPosition) >= 0,
            "start should be more or equal than 0",
            start)
        .validate(
            end -> ((int) end) >= start,
            "end should be more or equal than start",
            validator.getParameters().getIntOrDefault("end", Integer.MAX_VALUE));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    start = parameters.getInt("start");
    end = parameters.getIntOrDefault("end", Integer.MAX_VALUE);
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    String series = row.getString(0);
    collector.putString(
        row.getTime(),
        (end >= series.length())
            ? row.getString(0).substring(start)
            : row.getString(0).substring(start, end));
  }
}
