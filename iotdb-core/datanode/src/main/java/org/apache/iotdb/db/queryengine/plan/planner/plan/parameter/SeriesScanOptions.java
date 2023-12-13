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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SeriesScanOptions {

  private Filter globalTimeFilter;

  private Filter pushDownFilter;

  private final long pushDownLimit;
  private final long pushDownOffset;

  private final Set<String> allSensors;

  public SeriesScanOptions(
      Filter globalTimeFilter,
      Filter pushDownFilter,
      long pushDownLimit,
      long pushDownOffset,
      Set<String> allSensors) {
    this.globalTimeFilter = globalTimeFilter;
    this.pushDownFilter = pushDownFilter;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.allSensors = allSensors;
  }

  public static SeriesScanOptions getDefaultSeriesScanOptions(PartialPath seriesPath) {
    Builder builder = new Builder();
    if (seriesPath instanceof AlignedPath) {
      builder.withAllSensors(new HashSet<>(((AlignedPath) seriesPath).getMeasurementList()));
    } else {
      builder.withAllSensors(new HashSet<>(Collections.singletonList(seriesPath.getMeasurement())));
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
    return new PaginationController(pushDownLimit, pushDownOffset);
  }

  public void setTTL(long dataTTL) {
    this.globalTimeFilter = updateFilterUsingTTL(globalTimeFilter, dataTTL);
    if (this.pushDownFilter != null) {
      this.pushDownFilter = updateFilterUsingTTL(pushDownFilter, dataTTL);
    }
  }

  /** @return an updated filter concerning TTL */
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

  public static class Builder {

    private Filter globalTimeFilter = null;
    private Filter pushDownFilter = null;
    private long pushDownLimit = 0L;
    private long pushDownOffset = 0L;

    private Set<String> allSensors;

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

    public void withAllSensors(Set<String> allSensors) {
      this.allSensors = allSensors;
    }

    public SeriesScanOptions build() {
      return new SeriesScanOptions(
          globalTimeFilter, pushDownFilter, pushDownLimit, pushDownOffset, allSensors);
    }
  }
}
