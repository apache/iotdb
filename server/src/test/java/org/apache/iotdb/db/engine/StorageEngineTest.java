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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.util.List;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataRegion.class)
public class StorageEngineTest {

  private StorageEngine storageEngine;

  @Before
  public void setUp() {
    storageEngine = StorageEngine.getInstance();
  }

  @After
  public void after() {
    storageEngine = null;
  }

  @Test
  public void testGetAllDataRegionIds() throws Exception {
    DataRegionId id1 = new DataRegionId(1);
    DataRegion rg1 = PowerMockito.mock(DataRegion.class);
    DataRegion rg2 = PowerMockito.mock(DataRegion.class);
    DataRegionId id2 = new DataRegionId(2);
    storageEngine.setDataRegion(id1, rg1);
    storageEngine.setDataRegion(id2, rg2);

    List<DataRegionId> actual = Lists.newArrayList(id1, id2);
    List<DataRegionId> expect = storageEngine.getAllDataRegionIds();

    Assert.assertEquals(expect.size(), actual.size());
    Assert.assertTrue(actual.containsAll(expect));
    rg1.syncDeleteDataFiles();
    rg2.syncDeleteDataFiles();
  }
}
