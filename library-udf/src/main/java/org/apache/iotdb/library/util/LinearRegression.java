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
package org.apache.iotdb.library.util;

import org.apache.iotdb.udf.api.exception.UDFException;

import java.util.Arrays;

/**
 * This class offers function to do linear regression. Please use org.apache.commons.math3 for
 * regular usage.
 */
public class LinearRegression {
  double[] x, y, e, yhead;
  int n;
  double sumx, sumy, xbar, ybar, xxbar, yybar, xybar;
  double beta1, beta0, rss, ssr, R2, svar, svar1, svar0;

  public LinearRegression(double[] a, double[] b) throws Exception {
    x = a.clone();
    y = b.clone();
    n = x.length;
    if (x.length == 0 || y.length == 0) {
      throw new Exception("Empty input array(s).");
    }
    if (x.length != y.length) {
      throw new Exception("Different input array length.");
    }
    if (x.length == 1) { // cannot do regression
      throw new Exception("Input series should be longer than 1.");
    }
    e = new double[n];
    yhead = new double[n];
    sumx = Arrays.stream(x).sum();
    sumy = Arrays.stream(y).sum();
    xbar = sumx / n;
    ybar = sumy / n;
    // second pass: compute summary statistics
    xxbar = 0.0;
    yybar = 0.0;
    xybar = 0.0;
    for (int i = 0; i < n; i++) {
      xxbar += (x[i] - xbar) * (x[i] - xbar);
      yybar += (y[i] - ybar) * (y[i] - ybar);
      xybar += (x[i] - xbar) * (y[i] - ybar);
    }
    if (xxbar == 0d) {
      throw new UDFException("All input x are same.");
    }
    beta1 = xybar / xxbar;
    beta0 = ybar - beta1 * xbar;
    // analyze results
    int df = n - 2;
    rss = 0.0; // residual sum of squares
    ssr = 0.0; // regression sum of squares
    for (int i = 0; i < n; i++) {
      yhead[i] = beta1 * x[i] + beta0;
      e[i] = yhead[i] - y[i];
      rss += (yhead[i] - y[i]) * (yhead[i] - y[i]);
      ssr += (yhead[i] - ybar) * (yhead[i] - ybar);
    }
    R2 = ssr / yybar;
    svar = rss / df;
    svar1 = svar / xxbar;
    svar0 = svar / n + xbar * xbar * svar1;
  }

  public double getMSE() {
    return rss / n;
  }

  public double getMAbsE() { // mean abs error
    double sumAbsE = 0.0;
    for (int i = 0; i < n; i++) {
      sumAbsE += Math.abs(e[i]);
    }
    return sumAbsE / n;
  }

  public double[] getYhead() {
    return yhead;
  }
}
