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
package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * It's a virtual data region used for query which contains time series that don't belong to any
 * data region.
 */
public class VirtualDataRegion implements IDataRegionForQuery {

  private static final String VIRTUAL_DB_NAME = "root.__virtual";

  private static final QueryDataSource EMPTY_QUERY_DATA_SOURCE =
      new QueryDataSource(Collections.emptyList(), Collections.emptyList());

  private static final QueryDataSourceForRegionScan EMPTY_REGION_QUERY_DATA_SOURCE =
      new QueryDataSourceForRegionScan(Collections.emptyList(), Collections.emptyList());

  public static VirtualDataRegion getInstance() {
    return VirtualDataRegion.InstanceHolder.INSTANCE;
  }

  @Override
  public void readLock() {
    // do nothing, because itself is thread-safe already
  }

  @Override
  public void readUnlock() {
    // do nothing, because itself is thread-safe already
  }

  @Override
  public QueryDataSource query(
      List<IFullPath> pathList,
      IDeviceID singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      List<Long> timePartitions)
      throws QueryProcessException {
    return EMPTY_QUERY_DATA_SOURCE;
  }

  @Override
  public IQueryDataSource queryForDeviceRegionScan(
      Map<IDeviceID, DeviceContext> devicePathsToContext,
      QueryContext queryContext,
      Filter globalTimeFilter,
      List<Long> timePartitions)
      throws QueryProcessException {
    return EMPTY_REGION_QUERY_DATA_SOURCE;
  }

  @Override
  public IQueryDataSource queryForSeriesRegionScan(
      List<IFullPath> pathList,
      QueryContext queryContext,
      Filter globalTimeFilter,
      List<Long> timePartitions)
      throws QueryProcessException {
    return EMPTY_REGION_QUERY_DATA_SOURCE;
  }

  @Override
  public String getDatabaseName() {
    return VIRTUAL_DB_NAME;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final VirtualDataRegion INSTANCE = new VirtualDataRegion();
  }
}
