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

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This function replaces substring according to regex parameter from an input series. */
public class UDTFRegexReplace implements UDTF {

  private String regex;
  private Pattern pattern;
  private String replace;
  private int limit;
  private int offset;
  private boolean reverse;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.TEXT)
        .validate(
            regex -> ((String) regex).length() > 0,
            "regex should not be empty",
            validator.getParameters().getString("regex"))
        .validate(
            limit -> (int) limit >= -1,
            "limit has to be -1 for replacing all matches or non-negative integers for limited"
                + " times.",
            validator.getParameters().getIntOrDefault("limit", -1))
        .validate(
            offset -> (int) offset >= 0,
            "offset has to be non-negative to skip first several matches.",
            validator.getParameters().getIntOrDefault("offset", 0));
  }

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    regex = udfParameters.getString("regex");
    pattern = Pattern.compile(regex);
    replace = udfParameters.getString("replace");
    limit = udfParameters.getIntOrDefault("limit", -1);
    offset = udfParameters.getIntOrDefault("offset", 0);
    reverse = udfParameters.getBooleanOrDefault("reverse", false);
    udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    String origin = row.getString(0);
    Matcher matcher = pattern.matcher(origin);
    String result;
    if (reverse) {
      IntArrayList endIndexList = new IntArrayList();
      while (matcher.find()) {
        endIndexList.add(matcher.end());
      }
      String suffix;
      if (endIndexList.size() < offset + 1) {
        suffix = origin;
      } else {
        suffix = origin.substring(endIndexList.get(endIndexList.size() - offset - 1));
      }
      String prefix = "";
      if (limit != -1 && endIndexList.size() >= limit + offset + 1) {
        prefix = origin.substring(0, endIndexList.get(endIndexList.size() - limit - offset - 1));
      }
      result =
          prefix
              .concat(
                  origin
                      .substring(prefix.length(), origin.length() - suffix.length())
                      .replaceAll(regex, replace))
              .concat(suffix);
    } else {
      IntArrayList fromIndexList = new IntArrayList();
      while (matcher.find() && fromIndexList.size() < limit + offset + 2) {
        fromIndexList.add(matcher.start());
      }
      String prefix = origin;
      if (fromIndexList.size() >= offset + 1) {
        prefix = origin.substring(0, fromIndexList.get(offset));
      }
      String suffix = "";
      if (limit != -1 && fromIndexList.size() >= limit + offset + 1) {
        suffix = origin.substring(fromIndexList.get(limit + offset));
      }
      result =
          prefix
              .concat(
                  origin
                      .substring(prefix.length(), origin.length() - suffix.length())
                      .replaceAll(regex, replace))
              .concat(suffix);
    }
    collector.putString(row.getTime(), result);
  }
}
