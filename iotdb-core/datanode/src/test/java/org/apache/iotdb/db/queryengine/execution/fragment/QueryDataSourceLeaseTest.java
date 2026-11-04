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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

public class QueryDataSourceLeaseTest {

  @Test
  public void testCloseOnlyReleasesResourcesOnce() {
    FragmentInstanceContext context = Mockito.mock(FragmentInstanceContext.class);
    QueryDataSource dataSource =
        new QueryDataSource(Collections.emptyList(), Collections.emptyList());
    Set<TsFileResource> closedResources = Collections.emptySet();
    Set<TsFileResource> unclosedResources = Collections.emptySet();
    QueryDataSourceLease lease =
        new QueryDataSourceLease(dataSource, closedResources, unclosedResources, context);

    lease.close();
    lease.close();

    Mockito.verify(context, Mockito.times(1))
        .releaseBatchQueryDataSource(lease, closedResources, unclosedResources);
  }
}
