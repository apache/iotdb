/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.exception.IllegalPathException;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataNodeDevicePathCacheTest {

  DataNodeDevicePathCache dataNodeDevicePathCache;

  @Before
  public void setUp() throws Exception {
    dataNodeDevicePathCache = DataNodeDevicePathCache.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    dataNodeDevicePathCache.cleanUp();
  }

  @Test
  public void testGetPartialPath() {
    try {
      dataNodeDevicePathCache.getPartialPath("root.sg.d1");
    } catch (IllegalPathException e) {
      Assert.fail();
    }
  }

  @Test(expected = IllegalPathException.class)
  public void testGetIllegalPartialPath() throws Exception {
    try {
      dataNodeDevicePathCache.getPartialPath("root.sg.1");
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.1 is not a legal path", e.getMessage());
      throw e;
    }
    Assert.fail("root.sg.1 should be an illegal path");
  }
}
