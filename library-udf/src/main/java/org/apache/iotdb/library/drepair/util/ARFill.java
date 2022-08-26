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
package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.exception.UDFException;

public class ARFill extends ValueFill {
  // TODO Higer order AR regression
  private int order = 1;
  private double theta = 1e10;

  public ARFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
    calMeanAndVar();
  }

  public void setOrder(int order) {
    this.order = order;
  }

  @Override
  public void fill() throws UDFException {
    // compute \sum x_t * x_{t-1}
    double acf = 0;
    double factor = 0;
    int acf_cnt = 0;
    for (int i = 0; i < original.length - 1; i++) {
      double left = original[i], right = original[i + 1];
      if (Double.isNaN(left)) {
        left = 0;
      }
      if (Double.isNaN(right)) {
        right = 0;
      }
      acf += left * right;
      factor += left * left;
      acf_cnt += 1;
    }
    if (factor == 0d || this.theta >= 1) {
      this.time = new long[] {0};
      this.repaired = new double[] {0D};
      throw new UDFException("Cannot fit AR(1) model. Please try another method.");
    }
    // acf /= acf_cnt;
    this.theta = acf / factor;
    double mean_epsilon = 0;
    double var_epsilon = 0;
    double cnt_epsilon = 0;
    for (int i = 0; i < original.length - 1; i++) {
      double left = original[i], right = original[i + 1];
      if (Double.isNaN(left) || Double.isNaN(right)) {
        continue;
      }
      cnt_epsilon += 1;
      double epsilon = right - left * this.theta;
      mean_epsilon += epsilon;
      var_epsilon += epsilon * epsilon;
    }
    if (cnt_epsilon == 0d) {
      this.time = new long[] {0};
      this.repaired = new double[] {0D};
      throw new UDFException("Cannot fit AR(1) model. Please try another method.");
    }
    mean_epsilon /= cnt_epsilon;
    var_epsilon /= cnt_epsilon;
    for (int i = 0; i < original.length; i++) {
      double yt = original[i];
      if (!Double.isNaN(yt)) {
        repaired[i] = yt;
      } else {
        if (i != 0) {
          repaired[i] = this.theta * repaired[i - 1] + mean_epsilon;
        } else {
          repaired[i] = this.mean;
        }
      }
    }
  }
}
