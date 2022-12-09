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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;

import com.github.ggalmazor.ltdownsampling.LTThreeBuckets;
import com.github.ggalmazor.ltdownsampling.Point;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/** This function samples data by pool sampling. */
public class UDTFSample implements UDTF {

  enum Method {
    ISOMETRIC,
    RESERVOIR,
    TRIANGLE
  }

  private int k; // sample numbers
  private Method method;
  // These variables occurs in pool sampling
  private Pair<Long, Object>[] samples; // sampled data
  private int num = 0; // number of points already sampled
  private Random random;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validate(
            k -> (int) k > 0,
            "k should be a positive integer.",
            validator.getParameters().getIntOrDefault("k", 1))
        .validate(
            method ->
                "isometric".equalsIgnoreCase((String) method)
                    || "reservoir".equalsIgnoreCase((String) method)
                    || "triangle".equalsIgnoreCase((String) method),
            "Illegal sampling method.",
            validator.getParameters().getStringOrDefault("method", "reservoir"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    this.k = parameters.getIntOrDefault("k", 1);
    this.dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    String method = parameters.getStringOrDefault("method", "reservoir");
    if ("triangle".equalsIgnoreCase(method)) this.method = Method.TRIANGLE;
    else if ("isometric".equalsIgnoreCase(method)) this.method = Method.ISOMETRIC;
    else this.method = Method.RESERVOIR;
    if (this.method == Method.ISOMETRIC || this.method == Method.TRIANGLE) {
      configurations
          .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
          .setOutputDataType(parameters.getDataType(0));
    } else {
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(parameters.getDataType(0));
      this.samples = new Pair[this.k];
      this.random = new Random();
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    // pool sampling
    int x;
    if (this.num < this.k) {
      x = this.num;
    } else {
      x = random.nextInt(num + 1);
    }
    if (x < this.k) {
      Object v = Util.getValueAsObject(row);
      Long t = row.getTime();
      this.samples[x] = Pair.of(t, v);
    }
    this.num++;
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    // equal-distance sampling
    int n = rowWindow.windowSize();

    if (this.k < n) {
      if (this.method == Method.TRIANGLE) {
        List<Point> input = new LinkedList<>();
        for (int i = 0; i < n; i++) {
          Row row = rowWindow.getRow(i);
          BigDecimal time = BigDecimal.valueOf(row.getTime());
          BigDecimal data = BigDecimal.valueOf(Util.getValueAsDouble(row));
          input.add(new Point(time, data));
        }
        if (k > 2) {
          // The first and last element will always be sampled so the buckets is k - 2
          List<Point> output = LTThreeBuckets.sorted(input, k - 2);
          for (Point p : output) {
            switch (dataType) {
              case INT32:
                Util.putValue(collector, dataType, p.getX().longValue(), p.getY().intValue());
                break;
              case INT64:
                Util.putValue(collector, dataType, p.getX().longValue(), p.getY().longValue());
                break;
              case FLOAT:
                Util.putValue(collector, dataType, p.getX().longValue(), p.getY().floatValue());
                break;
              case DOUBLE:
                Util.putValue(collector, dataType, p.getX().longValue(), p.getY().doubleValue());
                break;
              default:
                throw new NoNumberException();
            }
          }
        } else { // For corner case of k == 1 and k == 2
          Row row = rowWindow.getRow(0); // Put first element
          Util.putValue(collector, dataType, row.getTime(), Util.getValueAsObject(row));
          if (k == 2) {
            row = rowWindow.getRow(n - 1); // Put last element
            Util.putValue(collector, dataType, row.getTime(), Util.getValueAsObject(row));
          }
        }
      } else { // Method.ISOMETRIC
        for (long i = 0; i < this.k; i++) {
          long j = Math.floorDiv(i * (long) n, (long) k); // avoid intermediate result overflows
          Row row = rowWindow.getRow((int) j);
          Util.putValue(collector, dataType, row.getTime(), Util.getValueAsObject(row));
        }
      }
    } else { // when k is larger than series length, output all points
      RowIterator iterator = rowWindow.getRowIterator();
      while (iterator.hasNextRow()) {
        Row row = iterator.next();
        Util.putValue(collector, dataType, row.getTime(), Util.getValueAsObject(row));
      }
    }
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    if (samples != null) { // for pool sampling only
      int m = Math.min(num, k);
      Arrays.sort(samples, 0, m);
      for (int i = 0; i < m; i++) {
        Pair<Long, Object> p = samples[i];
        Util.putValue(pc, dataType, p.getLeft(), p.getRight());
      }
    }
  }
}
