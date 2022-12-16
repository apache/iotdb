/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.type;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

/** used by timer and histogram. */
public interface HistogramSnapshot extends IMetric {

  /** get value by quantile */
  double getValue(double quantile);

  /** get values in snapshot */
  long[] getValues();

  /** get the size of values in snapshot */
  int size();

  /** get min value in values */
  long getMin();

  /** get median value in values */
  double getMedian();

  /** get mean value in values */
  double getMean();

  /** get max value of values */
  long getMax();

  /** writes the values of the snapshot to the given stream */
  void dump(OutputStream output);

  @Override
  default void constructValueMap(Map<String, Object> result) {
    result.put("max", getMax());
    result.put("sum", Arrays.stream(getValues()).sum());

    result.put("p0", getValue(0.0));
    result.put("p25", getValue(0.25));
    result.put("p50", getValue(0.5));
    result.put("p75", getValue(0.75));
    result.put("p100", getValue(1.0));
  }
}
