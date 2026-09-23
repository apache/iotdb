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

package org.apache.iotdb.db.storageengine.dataregion.read;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class QueryDataSourceRuntimeFilterTest {

  @Test
  public void testSeqInvalidatedIndicesAreIndependent() {
    TsFileResource f0 = mockResource(10, 10);
    TsFileResource f1 = mockResource(5, 5);
    TsFileResource f2 = mockResource(20, 20);
    QueryDataSource dataSource =
        new QueryDataSource(Arrays.asList(f0, f1, f2), Collections.emptyList());
    dataSource.initRuntimeFilterTracking();

    Assert.assertEquals(3, dataSource.getValidSize());

    dataSource.setSeqTsFileResourceInvalidated(0);
    dataSource.setSeqTsFileResourceInvalidated(2);

    Assert.assertTrue(dataSource.isRuntimeFilterPruned(true, 0));
    Assert.assertFalse(dataSource.isRuntimeFilterPruned(true, 1));
    Assert.assertTrue(dataSource.isRuntimeFilterPruned(true, 2));
    Assert.assertEquals(1, dataSource.getValidSize());
    Assert.assertTrue(dataSource.hasValidResource());
  }

  @Test
  public void testAllSeqInvalidatedMeansNoValidResource() {
    TsFileResource f0 = mockResource(1, 1);
    TsFileResource f1 = mockResource(2, 2);
    QueryDataSource dataSource =
        new QueryDataSource(Arrays.asList(f0, f1), Collections.emptyList());
    dataSource.initRuntimeFilterTracking();

    dataSource.setSeqTsFileResourceInvalidated(0);
    dataSource.setSeqTsFileResourceInvalidated(1);

    Assert.assertEquals(0, dataSource.getValidSize());
    Assert.assertFalse(dataSource.hasValidResource());
  }

  private static TsFileResource mockResource(long startTime, long endTime) {
    TsFileResource resource = Mockito.mock(TsFileResource.class);
    Mockito.when(resource.getFileStartTime()).thenReturn(startTime);
    Mockito.when(resource.isClosed()).thenReturn(true);
    Mockito.when(resource.getFileEndTime()).thenReturn(endTime);
    return resource;
  }
}
