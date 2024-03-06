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

package org.apache.iotdb.db.storageengine;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DevicePathCacheTests {

  @Before
  public void before() {}

  @Test
  public void test() {
    String path1 = "root.testdb.testd1.s1";
    String deviceId1 = DataNodeDevicePathCache.getInstance().getDeviceId("root.testdb.testd1.s1");
    Assert.assertEquals(path1, deviceId1);

    String path2 = "root.testdb.testd1.select";
    String deviceId2 =
        DataNodeDevicePathCache.getInstance().getDeviceId("root.testdb.testd1.select");
    Assert.assertEquals(path2, deviceId2);

    String path3 = "root.sg.`a``b`";
    String deviceId3 = DataNodeDevicePathCache.getInstance().getDeviceId("root.sg.`a``b`");
    Assert.assertEquals(path3, deviceId3);

    String path4 = "root.sg.`a.b`";
    String deviceId4 = DataNodeDevicePathCache.getInstance().getDeviceId("root.sg.`a.b`");
    Assert.assertEquals(path4, deviceId4);

    String path5 = "root.sg.`111`";
    String deviceId5 = DataNodeDevicePathCache.getInstance().getDeviceId("root.sg.`111`");
    Assert.assertEquals(path5, deviceId5);
  }

  @Test
  public void test02() {
    String path1 = "root.testdb.testd1.s1";
    String deviceId1 =
        DataNodeDevicePathCache.getInstance().getDeviceId(new String("root.testdb.testd1.s1"));
    String deviceId2 =
        DataNodeDevicePathCache.getInstance().getDeviceId(new String("root.testdb.testd1.s1"));
    Assert.assertEquals(path1, deviceId1);
    Assert.assertEquals(deviceId2, deviceId1);
  }
}
