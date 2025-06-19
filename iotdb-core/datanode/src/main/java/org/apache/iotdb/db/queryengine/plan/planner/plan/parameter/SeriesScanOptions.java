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
import java.util.concurrent.atomic.AtomicBoolean;

public class SeriesScanOptions {

  private Filter globalTimeFilter;
  private final Filter originalTimeFilter;

  private final AtomicBoolean timeFilterUpdatedByTtl = new AtomicBoolean(false);

  private final Filter pushDownFilter;

  private final long pushDownLimit;
  private final long pushDownOffset;

  private final Set<String> allSensors;

  private final boolean pushLimitToEachDevice;
  private PaginationController paginationController;
  private boolean isTableViewForTreeModel;
  private long ttlForTableView = Long.MAX_VALUE;

  public SeriesScanOptions(
      Filter globalTimeFilter,
      Filter pushDownFilter,
      long pushDownLimit,
      long pushDownOffset,
      Set<String> allSensors,
      boolean pushLimitToEachDevice,
      boolean isTableViewForTreeModel) {
    this.globalTimeFilter = globalTimeFilter;
    this.originalTimeFilter = globalTimeFilter;
    this.pushDownFilter = pushDownFilter;
    this.pushDownLimit = pushDownLimit;
    this.pushDownOffset = pushDownOffset;
    this.allSensors = allSensors;
    this.pushLimitToEachDevice = pushLimitToEachDevice;
    this.isTableViewForTreeModel = isTableViewForTreeModel;
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

  public long getPushDownLimit() {
    return this.pushDownLimit;
  }

  public long getPushDownOffset() {
    return this.pushDownOffset;
  }

  public boolean getPushLimitToEachDevice() {
    return this.pushLimitToEachDevice;
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

  public boolean timeFilterNeedUpdatedByTtl() {
    return !timeFilterUpdatedByTtl.get();
  }

  public void setTTLForTableDevice(long dataTTL) {
    // Devices in the table model share a same table ttl, so it only needs to be set once
    if (timeFilterUpdatedByTtl.compareAndSet(false, true)) {
      this.globalTimeFilter = updateFilterUsingTTL(globalTimeFilter, dataTTL);
    }
  }

  public void setTTLForTreeDevice(long dataTTL) {
    // ttlForTableView should be set before calling setTTL.
    // Different devices have different ttl, so we regenerate the globalTimeFilter each time
    this.globalTimeFilter =
        updateFilterUsingTTL(originalTimeFilter, Math.min(ttlForTableView, dataTTL));
  }

  public void setTTLForTableView(long ttlForTableView) {
    this.ttlForTableView = ttlForTableView;
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

  public boolean isTableViewForTreeModel() {
    return isTableViewForTreeModel;
  }

  public void setIsTableViewForTreeModel(boolean isTableViewForTreeModel) {
    this.isTableViewForTreeModel = isTableViewForTreeModel;
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
    private boolean isTableViewForTreeModel = false;

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

    public Builder withIsTableViewForTreeModel(boolean isTableViewForTreeModel) {
      this.isTableViewForTreeModel = isTableViewForTreeModel;
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
          pushLimitToEachDevice,
          isTableViewForTreeModel);
    }
  }
}
