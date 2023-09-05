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
  private double theta = 1e10;

  public ARFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
    calMeanAndVar();
  }

  @Override
  public void fill() throws UDFException {
    // Compute \sum x_t * x_{t-1}.
    double acf = 0;
    double factor = 0;
    for (int i = 0; i < original.length - 1; i++) {
      double left = original[i];
      double right = original[i + 1];
      if (Double.isNaN(left)) {
        left = 0;
      }
      if (Double.isNaN(right)) {
        right = 0;
      }
      acf += left * right;
      factor += left * left;
    }
    if (factor == 0d || this.theta >= 1) {
      this.time = new long[] {0};
      this.repaired = new double[] {0D};
      throw new UDFException("Cannot fit AR(1) model. Please try another method.");
    }
    this.theta = acf / factor;
    double meanEpsilon = 0;
    double cntEpsilon = 0;
    for (int i = 0; i < original.length - 1; i++) {
      double left = original[i];
      double right = original[i + 1];
      if (Double.isNaN(left) || Double.isNaN(right)) {
        continue;
      }
      cntEpsilon += 1;
      double epsilon = right - left * this.theta;
      meanEpsilon += epsilon;
    }
    if (cntEpsilon == 0d) {
      this.time = new long[] {0};
      this.repaired = new double[] {0D};
      throw new UDFException("Cannot fit AR(1) model. Please try another method.");
    }
    meanEpsilon /= cntEpsilon;
    for (int i = 0; i < original.length; i++) {
      double yt = original[i];
      if (!Double.isNaN(yt)) {
        repaired[i] = yt;
      } else {
        if (i != 0) {
          repaired[i] = this.theta * repaired[i - 1] + meanEpsilon;
        } else {
          repaired[i] = this.mean;
        }
      }
    }
  }
}
