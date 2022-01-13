/*
 * Copyright © 2021 iotdb-quality developer group (iotdb-quality@protonmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.quality.series;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.quality.series.util.ConsecutiveUtil;
import org.apache.iotdb.quality.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.tuple.Pair;

/** This function searches for all longest consecutive subsequences of input sereis. */
public class UDTFConsecutiveSequences implements UDTF {
  private ConsecutiveUtil consUtil;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validate(
        x -> (long) x > 0,
        "gap should be a time period whose unit is ms, s, m, h.",
        Util.parseTime(validator.getParameters().getStringOrDefault("gap", "1ms")));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.INT32);
    long gap = Util.parseTime(parameters.getStringOrDefault("gap", "0ms"));
    consUtil = new ConsecutiveUtil(-gap, -gap, gap);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (consUtil.getGap() == 0) {
      if (consUtil.getWindow().size() < consUtil.getMaxLen()) { // window is not full
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
    if (consUtil.getCount() > 1) {
      collector.putInt(consUtil.getFirst(), consUtil.getCount());
    }
  }
}
