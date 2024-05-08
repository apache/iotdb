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

/** This function does limited times of replacement of substring from an input series. */
public class UDTFStrReplace implements UDTF {

  private String target;
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
            target -> ((String) target).length() > 0,
            "target should not be empty",
            validator.getParameters().getString("target"))
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
    target = udfParameters.getString("target");
    replace = udfParameters.getString("replace");
    limit = udfParameters.getIntOrDefault("limit", -1);
    offset = udfParameters.getIntOrDefault("offset", 0);
    reverse = udfParameters.getBooleanOrDefault("reverse", false);
    udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    String origin = row.getString(0);
    String result;
    if (reverse) {
      int endIndex = origin.length();
      int count = 0;
      while (count < offset) {
        int matchIndex = origin.lastIndexOf(target, endIndex - 1);
        if (matchIndex == -1) {
          break;
        } else {
          endIndex = matchIndex;
          count++;
        }
      }
      // no enough matches to skip
      if (count < offset) {
        endIndex = 0;
      }
      String suffix = origin.substring(endIndex);
      while (count < offset + limit) {
        int matchIndex = origin.lastIndexOf(target, endIndex - 1);
        if (matchIndex == -1) {
          break;
        } else {
          endIndex = matchIndex;
          count++;
        }
      }
      // replace all left matches
      if (limit == -1 || count < offset + limit) {
        endIndex = 0;
      }
      String prefix = origin.substring(0, endIndex);
      result =
          prefix
              .concat(
                  origin
                      .substring(endIndex, origin.length() - suffix.length())
                      .replace(target, replace))
              .concat(suffix);
    } else {
      int fromIndex = 0;
      int count = 0;
      while (count < offset) {
        int matchIndex = origin.indexOf(target, fromIndex);
        if (matchIndex == -1) {
          break;
        } else {
          fromIndex = matchIndex + target.length();
          count++;
        }
      }
      // no enough matches to skip
      if (count < offset) {
        fromIndex = origin.length();
      }
      String prefix = origin.substring(0, fromIndex);
      while (count < offset + limit) {
        int matchIndex = origin.indexOf(target, fromIndex);
        if (matchIndex == -1) {
          break;
        } else {
          fromIndex = matchIndex + target.length();
          count++;
        }
      }
      // replace all left matches
      if (limit == -1 || count < offset + limit) {
        fromIndex = origin.length();
      }
      String suffix = origin.substring(fromIndex);
      result =
          prefix
              .concat(origin.substring(prefix.length(), fromIndex).replace(target, replace))
              .concat(suffix);
    }
    collector.putString(row.getTime(), result);
  }
}
