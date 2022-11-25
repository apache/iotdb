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

/** Used by timer and histogram. */
public interface HistogramSnapshot {

  /** Get value by quantile */
  double getValue(double quantile);

  /** Get values in snapshot */
  long[] getValues();

  /** Get the size of values in snapshot */
  int size();

  /** Get min value in values */
  long getMin();

  /** Get median value in values */
  double getMedian();

  /** Get mean value in values */
  double getMean();

  /** Get max value in values */
  long getMax();

  /** Writes the values of the snapshot to the given stream */
  void dump(OutputStream output);
}
