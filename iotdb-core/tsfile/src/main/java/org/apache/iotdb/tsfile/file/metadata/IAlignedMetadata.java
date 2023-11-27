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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;

import java.io.Serializable;

public interface IAlignedMetadata {

  TimeStatistics getTimeStatistics();

  Statistics<Serializable> getMeasurementStatistics(String measurement);

  /** @return whether there are any nulls. */
  default boolean hasNullValue(String measurement) {
    return getMeasurementStatistics(measurement).hasNullValue(getRowCount());
  }

  default long getRowCount() {
    return getTimeStatistics().getCount();
  }

  boolean isAllNulls(String measurement);
}
