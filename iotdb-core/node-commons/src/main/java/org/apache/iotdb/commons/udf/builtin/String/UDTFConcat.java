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
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.io.IOException;

/*This function Returns the concat string by input series and targets.
startsEnd: Indicates whether series behind targets. The default value is false.*/
public class UDTFConcat implements UDTF {

  private boolean seriesBehind;
  private final StringBuilder concatTargets = new StringBuilder();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    int size = validator.getParameters().getChildExpressions().size();
    for (int i = 0; i < size; i++) {
      validator.validateInputSeriesDataType(i, Type.TEXT, Type.STRING);
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    parameters
        .getAttributes()
        .forEach(
            (key, value) -> {
              if (key.startsWith("target") && value != null) concatTargets.append(value);
            });
    seriesBehind = parameters.getBooleanOrDefault("series_behind", false);
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    StringBuilder concatSeries = new StringBuilder();
    for (int i = 0; i < row.size(); i++) {
      if (row.isNull(i)) {
        continue;
      }
      concatSeries.append(row.getString(i));
    }

    collector.putString(
        row.getTime(),
        seriesBehind
            ? concatSeries.insert(0, concatTargets).toString()
            : concatSeries.append(concatTargets).toString());
  }

  @Override
  public Object transform(Row row) throws IOException {
    StringBuilder concatSeries = new StringBuilder();
    for (int i = 0; i < row.size(); i++) {
      if (row.isNull(i)) {
        continue;
      }
      concatSeries.append(row.getString(i));
    }

    String res =
        seriesBehind
            ? concatSeries.insert(0, concatTargets).toString()
            : concatSeries.append(concatTargets).toString();
    return BytesUtils.valueOf(res);
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    int colCount = columns.length;
    int rowCount = columns[0].getPositionCount();

    Binary[][] inputFrame = new Binary[colCount][rowCount];
    for (int i = 0; i < colCount - 1; i++) {
      inputFrame[i] = columns[i].getBinaries();
    }

    boolean[][] isNullFrame = new boolean[colCount][rowCount];
    for (int i = 0; i < colCount - 1; i++) {
      isNullFrame[i] = columns[i].isNull();
    }

    for (int row = 0; row < rowCount; row++) {
      StringBuilder concatSeries = new StringBuilder();
      for (int col = 0; col < colCount - 1; col++) {
        if (isNullFrame[col][row]) {
          continue;
        }
        String str = inputFrame[col][row].getStringValue(TSFileConfig.STRING_CHARSET);
        concatSeries.append(str);
      }

      String res =
          seriesBehind
              ? concatSeries.insert(0, concatTargets).toString()
              : concatSeries.append(concatTargets).toString();
      builder.writeBinary(BytesUtils.valueOf(res));
    }
  }
}
