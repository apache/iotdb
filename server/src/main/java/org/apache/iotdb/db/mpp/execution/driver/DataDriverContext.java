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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.IDataRegionForQuery;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.List;

/** TODO Add javadoc for context */
public class DataDriverContext extends DriverContext {

  private List<PartialPath> paths;
  private Filter timeFilter;
  private IDataRegionForQuery dataRegion;
  private List<DataSourceOperator> sourceOperators = new ArrayList<>();

  public DataDriverContext(
      FragmentInstanceContext fragmentInstanceContext,
      int pipelineId,
      Filter timeFilter,
      IDataRegionForQuery dataRegion) {
    super(fragmentInstanceContext);
    this.pipelineId = pipelineId;
    this.timeFilter = timeFilter;
    this.dataRegion = dataRegion;
  }

  // Only used for test
  public DataDriverContext(
      FragmentInstanceContext fragmentInstanceContext,
      List<PartialPath> paths,
      Filter timeFilter,
      IDataRegionForQuery dataRegion,
      List<DataSourceOperator> sourceOperators) {
    super(fragmentInstanceContext);
    this.paths = paths;
    this.timeFilter = timeFilter;
    this.dataRegion = dataRegion;
    this.sourceOperators = sourceOperators;
  }

  public DataDriverContext(DataDriverContext parentContext) {
    super(parentContext.getFragmentInstanceContext());
    this.timeFilter = parentContext.timeFilter;
    this.dataRegion = parentContext.dataRegion;
  }

  public void addPath(PartialPath path) {
    if (this.paths == null) {
      this.paths = new ArrayList<>();
    }
    this.paths.add(path);
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public void addSourceOperator(DataSourceOperator sourceOperator) {
    this.sourceOperators.add(sourceOperator);
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public IDataRegionForQuery getDataRegion() {
    return dataRegion;
  }

  public List<DataSourceOperator> getSourceOperators() {
    return sourceOperators;
  }

  public DriverContext createSubDriverContext() {
    return new DataDriverContext(this);
  }
}
