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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.source.DataSourceOperator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

public class DataDriverContext extends DriverContext {
  private final List<PartialPath> paths;
  private final Filter timeFilter;
  private final VirtualStorageGroupProcessor dataRegion;
  private final List<DataSourceOperator> sourceOperators;

  public DataDriverContext(
      FragmentInstanceContext fragmentInstanceContext,
      List<PartialPath> paths,
      Filter timeFilter,
      VirtualStorageGroupProcessor dataRegion,
      List<DataSourceOperator> sourceOperators) {
    super(fragmentInstanceContext);
    this.paths = paths;
    this.timeFilter = timeFilter;
    this.dataRegion = dataRegion;
    this.sourceOperators = sourceOperators;
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public VirtualStorageGroupProcessor getDataRegion() {
    return dataRegion;
  }

  public List<DataSourceOperator> getSourceOperators() {
    return sourceOperators;
  }
}
