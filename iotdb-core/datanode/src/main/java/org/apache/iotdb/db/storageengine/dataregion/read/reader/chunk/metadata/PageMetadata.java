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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.tsfile.utils.Preconditions.checkArgument;

public class PageMetadata implements IMetadata {
  private final String measurementUid;
  private final TSDataType tsDataType;
  private Statistics<? extends Serializable> statistics;
  private boolean modified;

  public PageMetadata(
      String measurementUid, TSDataType tsDataType, Statistics<? extends Serializable> statistics) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return String.format(
        "measurementId: %s, datatype: %s, " + "Statistics: %s",
        measurementUid, tsDataType, statistics);
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }

  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.statistics = statistics;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    checkArgument(
        measurementIndex == 0,
        "Non-aligned chunk only has one measurement, but measurementIndex is " + measurementIndex);
    return Optional.ofNullable(statistics);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return false;
  }
}
