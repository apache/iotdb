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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata;

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;
import java.util.Optional;

public class AlignedPageMetadata implements IMetadata {
  private Statistics<? extends Serializable> timeStatistics;
  private Statistics<? extends Serializable>[] valueStatistics;

  public AlignedPageMetadata(
      Statistics<? extends Serializable> timeStatistics,
      Statistics<? extends Serializable>[] valuesStatistics) {
    this.timeStatistics = timeStatistics;
    this.valueStatistics = valuesStatistics;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return valueStatistics.length == 1 && valueStatistics[0] != null
        ? valueStatistics[0]
        : timeStatistics;
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return timeStatistics;
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    return Optional.ofNullable(
        measurementIndex >= valueStatistics.length ? null : valueStatistics[measurementIndex]);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    if (measurementIndex >= valueStatistics.length) {
      return false;
    }
    long rowCount = getTimeStatistics().getCount();
    Statistics<? extends Serializable> stats = valueStatistics[measurementIndex];
    return stats != null && stats.hasNullValue(rowCount);
  }

  public void setStatistics(
      Statistics<? extends Serializable> timeStatistics,
      Statistics<? extends Serializable>[] valueStatistics) {
    this.timeStatistics = timeStatistics;
    this.valueStatistics = valueStatistics;
  }
}
