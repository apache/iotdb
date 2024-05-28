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

import java.io.IOException;

/*This function returns position of target in an input series.*/
public class UDTFStrLocate implements UDTF {
  private String target;
  private boolean reverse;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.TEXT, Type.STRING)
        .validate(
            target -> ((String) target).length() > 0,
            "target should not be empty",
            validator.getParameters().getString("target"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    target = parameters.getString("target");
    reverse = parameters.getBooleanOrDefault("reverse", false);
    configurations
        .setAccessStrategy(new MappableRowByRowAccessStrategy())
        .setOutputDataType(Type.INT32);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    collector.putInt(
        row.getTime(),
        reverse ? row.getString(0).lastIndexOf(target) : row.getString(0).indexOf(target));
  }

  @Override
  public Object transform(Row row) throws IOException {
    if (row.isNull(0)) {
      return null;
    }
    return reverse ? row.getString(0).lastIndexOf(target) : row.getString(0).indexOf(target);
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws Exception {
    Binary[] inputs = columns[0].getBinaries();
    boolean[] isNulls = columns[0].isNull();

    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        String str = inputs[i].getStringValue(TSFileConfig.STRING_CHARSET);
        int res = reverse ? str.lastIndexOf(target) : str.indexOf(target);
        builder.writeInt(res);
      }
    }
  }
}
