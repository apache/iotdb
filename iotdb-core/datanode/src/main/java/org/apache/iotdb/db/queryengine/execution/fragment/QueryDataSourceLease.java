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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the TsFile reader references retained by one batch QueryDataSource. */
public final class QueryDataSourceLease implements AutoCloseable {

  private final QueryDataSource dataSource;
  private final Set<TsFileResource> closedResources;
  private final Set<TsFileResource> unclosedResources;
  private final FragmentInstanceContext instanceContext;
  private final AtomicBoolean closed = new AtomicBoolean();

  QueryDataSourceLease(
      QueryDataSource dataSource,
      Set<TsFileResource> closedResources,
      Set<TsFileResource> unclosedResources,
      FragmentInstanceContext instanceContext) {
    this.dataSource = dataSource;
    this.closedResources = closedResources;
    this.unclosedResources = unclosedResources;
    this.instanceContext = instanceContext;
  }

  public QueryDataSource getDataSource() {
    return dataSource;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      instanceContext.releaseBatchQueryDataSource(this, closedResources, unclosedResources);
    }
  }
}
