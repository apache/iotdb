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

/** used by Timer and Histogram. */
public interface HistogramSnapshot {

  /** Get value by quantile */
  double getValue(double quantile);

  /** Get values in snapshot */
  long[] getValues();

  /** Get size of value in snapshot */
  int size();

  /** Get median of values */
  double getMedian();

  /** Get min of values */
  long getMin();

  /** Get mean of values */
  double getMean();

  /** Get max of values */
  long getMax();

  /**
   * Writes the values of the snapshot to the given stream.
   *
   * @param output an output stream
   */
  void dump(OutputStream output);
}
