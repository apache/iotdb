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
package org.apache.iotdb.library.frequency.util;

import org.apache.iotdb.udf.api.collector.PointCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util for UDFFFT */
public class FFTUtil {
  private final String result;
  private final double compressRate;
  private static final Logger logger = LoggerFactory.getLogger(FFTUtil.class);

  public FFTUtil(String res, double cmprate) {
    this.result = res;
    this.compressRate = cmprate;
  }

  public void outputCompressed(PointCollector collector, double a[]) throws Exception {
    int n = a.length / 2;
    // calculate total energy
    double sum = 0;
    for (int i = 0; i < n; i++) {
      sum += a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1];
    }
    // compress
    double temp = a[0] * a[0] + a[1] * a[1];
    add(collector, a, 0);
    for (int i = 1; i <= n / 2; i++) {
      add(collector, a, i);
      temp += (a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1]) * 2;
      if (temp > compressRate * sum) {
        if (logger.isDebugEnabled()) {
          logger.debug(String.valueOf(i));
        }
        break;
      }
    }
    // sign at the tail for length
    add(collector, a, n - 1);
  }

  public void add(PointCollector collector, double a[], int i) throws Exception {
    double ans = 0;
    switch (result) {
      case "real":
        ans = a[i * 2];
        break;
      case "imag":
        ans = a[i * 2 + 1];
        break;
      case "abs":
        ans = Math.sqrt(a[i * 2] * a[i * 2] + a[2 * i + 1] * a[2 * i + 1]);
        break;
      case "angle":
        ans = Math.atan2(a[2 * i + 1], a[2 * i]);
        break;
      default:
        throw new Exception("It's impossible");
    }
    collector.putDouble(i, ans);
  }

  public void outputUncompressed(PointCollector collector, double a[]) throws Exception {
    int n = a.length / 2;
    for (int i = 0; i < n; i++) {
      add(collector, a, i);
    }
  }
}
