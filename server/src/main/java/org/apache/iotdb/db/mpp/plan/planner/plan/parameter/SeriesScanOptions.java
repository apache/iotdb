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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class SeriesScanOptions {

  private Filter globalTimeFilter;

  private Filter queryFilter;

  private final long limit;
  private final long offset;

  private final Set<String> allSensors;

  public SeriesScanOptions(
      Filter globalTimeFilter,
      Filter queryFilter,
      long limit,
      long offset,
      Set<String> allSensors) {
    this.globalTimeFilter = globalTimeFilter;
    if (!Objects.equals(globalTimeFilter, queryFilter)) {
      this.queryFilter = queryFilter;
    }
    this.limit = limit;
    this.offset = offset;
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

  public Filter getQueryFilter() {
    return queryFilter;
  }

  public long getLimit() {
    return limit;
  }

  public long getOffset() {
    return offset;
  }

  public Set<String> getAllSensors() {
    return allSensors;
  }

  public PaginationController getPaginationController() {
    return new PaginationController(limit, offset);
  }

  public void setTTL(long dataTTL) {
    this.globalTimeFilter = updateFilterUsingTTL(globalTimeFilter, dataTTL);
    if (this.queryFilter != null) {
      this.queryFilter = updateFilterUsingTTL(queryFilter, dataTTL);
    }
  }

  /** @return an updated filter concerning TTL */
  public static Filter updateFilterUsingTTL(Filter filter, long dataTTL) {
    if (dataTTL != Long.MAX_VALUE) {
      if (filter != null) {
        filter = new AndFilter(filter, TimeFilter.gtEq(DateTimeUtils.currentTime() - dataTTL));
      } else {
        filter = TimeFilter.gtEq(DateTimeUtils.currentTime() - dataTTL);
      }
    }
    return filter;
  }

  public static class Builder {

    private Filter globalTimeFilter = null;
    private Filter queryFilter = null;
    private long limit = 0L;
    private long offset = 0L;

    private Set<String> allSensors;

    public Builder withGlobalTimeFilter(Filter globalTimeFilter) {
      this.globalTimeFilter = globalTimeFilter;
      return this;
    }

    public Builder withQueryFilter(Filter queryFilter) {
      this.queryFilter = queryFilter;
      return this;
    }

    public Builder withLimit(long limit) {
      this.limit = limit;
      return this;
    }

    public Builder withOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public void withAllSensors(Set<String> allSensors) {
      this.allSensors = allSensors;
    }

    public SeriesScanOptions build() {
      return new SeriesScanOptions(globalTimeFilter, queryFilter, limit, offset, allSensors);
    }
  }
}
