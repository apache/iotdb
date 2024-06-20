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

package org.apache.iotdb.db.queryengine.execution.driver;

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataDriverContext extends DriverContext {

  // it will be set to null, after being merged into Parent FIContext
  private List<IFullPath> paths;
  private QueryDataSourceType queryDataSourceType = null;
  private Map<IDeviceID, DeviceContext> deviceIDToContext;
  // it will be set to null, after QueryDataSource being inited
  private List<DataSourceOperator> sourceOperators;

  public DataDriverContext(FragmentInstanceContext fragmentInstanceContext, int pipelineId) {
    super(fragmentInstanceContext, pipelineId);
    this.paths = new ArrayList<>();
    this.sourceOperators = new ArrayList<>();
    this.deviceIDToContext = null;
  }

  public DataDriverContext(DataDriverContext parentContext, int pipelineId) {
    super(parentContext.getFragmentInstanceContext(), pipelineId);
    this.paths = new ArrayList<>();
    this.sourceOperators = new ArrayList<>();
    this.deviceIDToContext = null;
  }

  public void setQueryDataSourceType(QueryDataSourceType queryDataSourceType) {
    this.queryDataSourceType = queryDataSourceType;
  }

  public void setDeviceIDToContext(Map<IDeviceID, DeviceContext> deviceIDToContext) {
    this.deviceIDToContext = deviceIDToContext;
  }

  public void clearDeviceIDToContext() {
    // friendly for gc
    deviceIDToContext = null;
  }

  public void addPath(IFullPath path) {
    this.paths.add(path);
  }

  public void addSourceOperator(DataSourceOperator sourceOperator) {
    this.sourceOperators.add(sourceOperator);
  }

  public List<IFullPath> getPaths() {
    return paths;
  }

  public Map<IDeviceID, DeviceContext> getDeviceIDToContext() {
    return deviceIDToContext;
  }

  public Optional<QueryDataSourceType> getQueryDataSourceType() {
    return Optional.ofNullable(queryDataSourceType);
  }

  public void clearPaths() {
    // friendly for gc
    paths = null;
  }

  public IDataRegionForQuery getDataRegion() {
    return getFragmentInstanceContext().getDataRegion();
  }

  public IQueryDataSource getSharedQueryDataSource() throws QueryProcessException {
    return getFragmentInstanceContext().getSharedQueryDataSource();
  }

  public List<DataSourceOperator> getSourceOperators() {
    return sourceOperators;
  }

  public void clearSourceOperators() {
    // friendly for gc
    sourceOperators = null;
  }

  @Override
  public DriverContext createSubDriverContext(int pipelineId) {
    return new DataDriverContext(this, pipelineId);
  }
}
