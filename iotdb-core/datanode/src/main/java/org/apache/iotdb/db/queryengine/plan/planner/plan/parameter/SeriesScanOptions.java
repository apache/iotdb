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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.reader.series.PaginationController;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SeriesScanOptions {

  private Filter globalTimeFilter;

  private final Filter pushDownFilter;

  private final long pushDownLimit;
  private final long pushDownOffset;

  private final Set<String> allSensors;

  private final boolean pushLimitToEachDevice;
  private PaginationController paginationController;

  public SeriesScanOptions(
      Filter globalTimeFilter,
      Filter pushDownFilter,
      long pushDownLimit,
      long pushDownOffset,
      Set<String> allSensors,
      boolean pushLimitToEachDevice) {
    this.globalTimeFilter = globalTimeFilter;
    this.pushDownFilter = pushDownFilter;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.allSensors = allSensors;
    this.pushLimitToEachDevice = pushLimitToEachDevice;
  }

  public static SeriesScanOptions getDefaultSeriesScanOptions(IFullPath seriesPath) {
    Builder builder = new Builder();
    if (seriesPath instanceof AlignedFullPath) {
      builder.withAllSensors(new HashSet<>(((AlignedFullPath) seriesPath).getMeasurementList()));
    } else {
      builder.withAllSensors(
          new HashSet<>(
              Collections.singletonList(((NonAlignedFullPath) seriesPath).getMeasurement())));
    }
    return builder.build();
  }

  public Filter getGlobalTimeFilter() {
    return globalTimeFilter;
  }

  public Filter getPushDownFilter() {
    return pushDownFilter;
  }

  public Set<String> getAllSensors() {
    return allSensors;
  }

  public PaginationController getPaginationController() {
    if (pushLimitToEachDevice) {
      return new PaginationController(pushDownLimit, pushDownOffset);
    } else {
      if (paginationController == null) {
        paginationController = new PaginationController(pushDownLimit, pushDownOffset);
      }
      return paginationController;
    }
  }

  public void setTTL(long dataTTL) {
    this.globalTimeFilter = updateFilterUsingTTL(globalTimeFilter, dataTTL);
  }

  /**
   * @return an updated filter concerning TTL
   */
  public static Filter updateFilterUsingTTL(Filter filter, long dataTTL) {
    if (dataTTL != Long.MAX_VALUE) {
      if (filter != null) {
        filter =
            FilterFactory.and(
                filter, TimeFilterApi.gtEq(CommonDateTimeUtils.currentTime() - dataTTL));
      } else {
        filter = TimeFilterApi.gtEq(CommonDateTimeUtils.currentTime() - dataTTL);
      }
    }
    return filter;
  }

  /**
   * pushLimitToEachDevice==false means that all devices return total limit rows.
   *
   * @return true only if pushLimitToEachDevice==false and limit in paginationController has already
   *     consumed up
   */
  public boolean limitConsumedUp() {
    return !pushLimitToEachDevice
        && (paginationController != null && !paginationController.hasCurLimit());
  }

  public static class Builder {

    private Filter globalTimeFilter = null;
    private Filter pushDownFilter = null;
    private long pushDownLimit = 0L;
    private long pushDownOffset = 0L;

    private Set<String> allSensors;

    private boolean pushLimitToEachDevice = true;

    public Builder withGlobalTimeFilter(Filter globalTimeFilter) {
      this.globalTimeFilter = globalTimeFilter;
      return this;
    }

    public Builder withPushDownFilter(Filter pushDownFilter) {
      this.pushDownFilter = pushDownFilter;
      return this;
    }

    public Builder withPushDownLimit(long pushDownLimit) {
      this.pushDownLimit = pushDownLimit;
      return this;
    }

    public Builder withPushDownOffset(long pushDownOffset) {
      this.pushDownOffset = pushDownOffset;
      return this;
    }

    public Builder withPushLimitToEachDevice(boolean pushLimitToEachDevice) {
      this.pushLimitToEachDevice = pushLimitToEachDevice;
      return this;
    }

    public void withAllSensors(Set<String> allSensors) {
      this.allSensors = allSensors;
    }

    public SeriesScanOptions build() {
      return new SeriesScanOptions(
          globalTimeFilter,
          pushDownFilter,
          pushDownLimit,
          pushDownOffset,
          allSensors,
          pushLimitToEachDevice);
    }
  }
}
