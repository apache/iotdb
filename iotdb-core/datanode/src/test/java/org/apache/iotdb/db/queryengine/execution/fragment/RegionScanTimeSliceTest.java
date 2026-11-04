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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RegionScanTimeSliceTest {
  private int originalTimeSlice;

  @Before
  public void setUp() {
    originalTimeSlice =
        CommonDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs();
  }

  @After
  public void tearDown() {
    CommonDescriptor.getInstance()
        .getConfig()
        .setDriverTaskExecutionTimeSliceInMs(originalTimeSlice);
  }

  @Test
  public void exhaustedSeriesScanTimeSliceStillUnlocksRegion() throws Exception {
    DataRegion realRegion = new DataRegion("root.region_release", "1");
    DataRegion region = spy(realRegion);
    FakedFragmentInstanceContext context = new FakedFragmentInstanceContext(null, region);
    CommonDescriptor.getInstance().getConfig().setDriverTaskExecutionTimeSliceInMs(1);
    AtomicBoolean locked = new AtomicBoolean();
    doAnswer(
            invocation -> {
              assertTrue(realRegion.tryReadLock(10_000));
              locked.set(true);
              Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
              return true;
            })
        .when(region)
        .tryReadLock(anyLong());
    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              locked.set(false);
              return null;
            })
        .when(region)
        .readUnlock();
    try {
      assertFalse(
          context.initRegionScanQueryDataSource(Collections.singletonList(mock(IFullPath.class))));
      assertFalse(
          "region read lock must be released on the exhausted-time-slice return", locked.get());
      verify(region).readUnlock();
      assertEquals(0, context.getInitQueryDataSourceRetryCount());
    } finally {
      if (locked.get()) {
        region.readUnlock();
      }
    }
  }
}
