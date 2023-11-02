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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.tsfile.access.Column;
import org.apache.iotdb.tsfile.access.ColumnBuilder;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.regex.Pattern;

public class UDTFMatches implements UDTF {

  private Pattern pattern;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateRequiredAttribute("regex")
        .validateInputSeriesDataType(0, Type.TEXT);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    pattern = Pattern.compile(parameters.getString("regex"));
    configurations.setAccessStrategy(new MappableRowByRowAccessStrategy()).setOutputDataType(Type.BOOLEAN);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    collector.putBoolean(row.getTime(), pattern.matcher(row.getString(0)).matches());
  }

  @Override
  public Object transform(Row row) throws IOException {
    if (row.isNull(0)) {
      return null;
    }
    return pattern.matcher(row.getString(0)).matches();
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
        builder.writeBoolean(pattern.matcher(str).matches());
      }
    }
  }
}
