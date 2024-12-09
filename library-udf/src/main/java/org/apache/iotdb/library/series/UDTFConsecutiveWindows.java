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

package org.apache.iotdb.library.series;

import org.apache.iotdb.library.series.util.ConsecutiveUtil;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.lang3.tuple.Pair;

/** This function searches for consecutive subsequences of given length of input sereis. */
public class UDTFConsecutiveWindows implements UDTF {
  private ConsecutiveUtil consUtil;
  private static final int MAX_LEN = 128;
  private static final String TIMESTAMP_PRECISION = "timestampPrecision";
  public static final String MS_PRECISION = "ms";
  private long len;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    String timestampPrecision =
        validator.getParameters().getSystemStringOrDefault(TIMESTAMP_PRECISION, MS_PRECISION);
    validator
        .validate(
            x -> (long) x > 0,
            "gap should be a time period whose unit is ms, s, m, h.",
            Util.parseTime(
                validator.getParameters().getStringOrDefault("gap", "1ms"), timestampPrecision))
        .validate(
            x -> (long) x > 0,
            "length should be a time period whose unit is ms, s, m, h.",
            Util.parseTime(validator.getParameters().getString("length"), timestampPrecision));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT32);
    String timestampPrecision =
        parameters.getSystemStringOrDefault(TIMESTAMP_PRECISION, MS_PRECISION);
    long gap = Util.parseTime(parameters.getStringOrDefault("gap", "0ms"), timestampPrecision);
    len = Util.parseTime(parameters.getString("length"), timestampPrecision);
    int count = gap == 0 ? 0 : (int) (len / gap + 1);
    consUtil = new ConsecutiveUtil(-gap, -gap, gap);
    consUtil.setCount(count);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (consUtil.getGap() == 0) {
      if (consUtil.getWindow().size() < MAX_LEN) { // window is not full
        consUtil.getWindow().add(Pair.of(row.getTime(), consUtil.check(row)));
      } else {
        consUtil.calculateGap();
        consUtil.cleanWindow(collector);
      }
    } else {
      consUtil.process(row.getTime(), consUtil.check(row), collector);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (consUtil.getGap() == 0) {
      consUtil.calculateGap();
      consUtil.cleanWindow(collector);
    }
    for (;
        consUtil.getFirst() + len <= consUtil.getLast();
        consUtil.setFirst(consUtil.getFirst() + consUtil.getGap())) {
      collector.putInt(consUtil.getFirst(), consUtil.getCount());
    }
  }
}
