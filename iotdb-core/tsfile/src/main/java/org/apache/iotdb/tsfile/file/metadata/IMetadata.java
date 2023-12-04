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

import java.io.Serializable;
import java.util.Optional;

public interface IMetadata {

  <T extends Serializable> Statistics<T> getStatistics();

  <T extends Serializable> Statistics<T> getTimeStatistics();

  <T extends Serializable> Optional<Statistics<T>> getMeasurementStatistics(int measurementIndex);

  int getMeasurementCount();

  default boolean hasNullValue(int measurementIndex) {
    long rowCount = getTimeStatistics().getCount();
    Optional<Statistics<Serializable>> statistics =
        getMeasurementStatistics(measurementIndex);
    return statistics.map(stat -> stat.hasNullValue(rowCount)).orElse(true);
  }

  default boolean isAllNulls(int measurementIndex) {
    return false;
  }

  default boolean timeAllSelected() {
    for (int index = 0; index < getMeasurementCount(); index++) {
      if (!hasNullValue(index)) {
        // When there is any value page point number that is the same as the time page,
        // it means that all timestamps in time page will be selected.
        return true;
      }
    }
    return false;
  }
}
