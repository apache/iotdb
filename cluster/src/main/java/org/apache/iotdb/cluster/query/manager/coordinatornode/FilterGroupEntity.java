/**
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
package org.apache.iotdb.cluster.query.manager.coordinatornode;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterFilterSeriesReader;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Filter entities of a data group, concluding QueryPlan, filters, all filter paths and filter readers
 */
public class FilterGroupEntity {

  /**
   * Group id
   */
  private String groupId;

  /**
   * Query plans of filter paths which are divided from queryPlan
   */
  private QueryPlan queryPlan;

  /**
   * Filters of filter path.
   */
  private List<Filter> filters;

  /**
   *
   * all filter series
   * <p>
   * Note: It may contain multiple series in a complicated tree
   * for example: select * from root.vehicle where d0.s0 > 10 and d0.s0 < 101 or time = 12,
   * filter tree: <code>[[[[root.vehicle.d0.s0:time == 12] || [root.vehicle.d0.s1:time == 12]] || [root.vehicle.d1.s2:time == 12]] || [root.vehicle.d1.s3:time == 12]]</code>
   * </p>
   */
  private List<Path> filterPaths;


  /**
   * Series reader of filter paths (only contains remote series)
   */
  private List<ClusterFilterSeriesReader> filterSeriesReaders;

  public FilterGroupEntity(String groupId) {
    this.groupId = groupId;
    this.filterPaths = new ArrayList<>();
    this.filters = new ArrayList<>();
    this.filterSeriesReaders = new ArrayList<>();
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public void setQueryPlan(QueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void addFilter(Filter filter) {
    this.filters.add(filter);
  }

  public List<Path> getFilterPaths() {
    return filterPaths;
  }

  public void addFilterPaths(Path filterPath) {
    this.filterPaths.add(filterPath);
  }

  public List<ClusterFilterSeriesReader> getFilterSeriesReaders() {
    return filterSeriesReaders;
  }

  public void addFilterSeriesReader(ClusterFilterSeriesReader filterSeriesReader) {
    this.filterSeriesReaders.add(filterSeriesReader);
  }
}
