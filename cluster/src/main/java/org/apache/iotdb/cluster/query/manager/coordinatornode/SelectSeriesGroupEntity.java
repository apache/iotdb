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
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Select series entity entities of a data group, concluding QueryPlan, all select paths and series readers
 */
public class SelectSeriesGroupEntity {
  /**
   * Group id
   */
  private String groupId;

  /**
   * Query plans of filter paths which are divided from queryPlan
   */
  private QueryPlan queryPlan;

  /**
   *
   * all select series
   * <p>
   * Note: It may contain multiple series in a query
   * for example: select sum(s0), max(s0) from root.vehicle.d0 where s0 > 10
   * </p>
   */
  private List<Path> selectPaths;

  /**
   * Series reader of filter paths (only contains remote series)
   */
  private List<ClusterSelectSeriesReader> selectSeriesReaders;

  public SelectSeriesGroupEntity(String groupId) {
    this.groupId = groupId;
    this.selectPaths = new ArrayList<>();
    this.selectSeriesReaders = new ArrayList<>();
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

  public List<Path> getSelectPaths() {
    return selectPaths;
  }

  public void addSelectPaths(Path selectPath) {
    this.selectPaths.add(selectPath);
  }

  public List<ClusterSelectSeriesReader> getSelectSeriesReaders() {
    return selectSeriesReaders;
  }

  public void addSelectSeriesReader(ClusterSelectSeriesReader selectSeriesReader) {
    this.selectSeriesReaders.add(selectSeriesReader);
  }
}
