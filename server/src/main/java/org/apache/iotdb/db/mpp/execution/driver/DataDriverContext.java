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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.IDataRegionForQuery;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;

import java.util.ArrayList;
import java.util.List;

/** TODO Add javadoc for context */
public class DataDriverContext extends DriverContext {

  private final List<PartialPath> paths;
  private final List<DataSourceOperator> sourceOperators;

  public DataDriverContext(FragmentInstanceContext fragmentInstanceContext, int pipelineId) {
    super(fragmentInstanceContext, pipelineId);
    this.paths = new ArrayList<>();
    this.sourceOperators = new ArrayList<>();
  }

  public DataDriverContext(DataDriverContext parentContext, int pipelineId) {
    super(parentContext.getFragmentInstanceContext(), pipelineId);
    this.paths = new ArrayList<>();
    this.sourceOperators = new ArrayList<>();
  }

  public void addPath(PartialPath path) {
    this.paths.add(path);
  }

  public void addSourceOperator(DataSourceOperator sourceOperator) {
    this.sourceOperators.add(sourceOperator);
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public IDataRegionForQuery getDataRegion() {
    return getFragmentInstanceContext().getDataRegion();
  }

  public QueryDataSource getSharedQueryDataSource() throws QueryProcessException {
    return getFragmentInstanceContext().getSharedQueryDataSource();
  }

  public List<DataSourceOperator> getSourceOperators() {
    return sourceOperators;
  }

  @Override
  public DriverContext createSubDriverContext(int pipelineId) {
    return new DataDriverContext(this, pipelineId);
  }
}
