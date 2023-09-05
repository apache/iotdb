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

package org.apache.iotdb.library.string;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This function matches substring according to given regex from an input series. */
public class UDTFRegexMatch implements UDTF {
  private Pattern pattern;
  private int group;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.TEXT)
        .validate(
            regex -> ((String) regex).length() > 0,
            "regexp has to be a valid regular expression.",
            validator.getParameters().getStringOrDefault("regex", ""))
        .validate(
            group -> (int) group >= 0,
            "group index has to be a non-negative integer.",
            validator.getParameters().getIntOrDefault("group", 0));
  }

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    pattern = Pattern.compile(udfParameters.getString("regex"));
    group = udfParameters.getIntOrDefault("group", 0);
    udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Matcher matcher = pattern.matcher(row.getString(0));
    if (matcher.find() && matcher.groupCount() >= group) {
      collector.putString(row.getTime(), matcher.group(group));
    }
  }
}
