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
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HashVirtualPartitionerTest {
  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    // init file dir
    StorageEngine.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void basicTest() throws IllegalPathException {
    HashVirtualPartitioner hashVirtualPartitioner = HashVirtualPartitioner.getInstance();

    // sg -> deviceId
    HashMap<PartialPath, Set<PartialPath>> realMap = new HashMap<>();
    PartialPath d1 = new PartialPath("root.sg1.d1");
    PartialPath d2 = new PartialPath("root.sg1.d2");

    int sg1 = hashVirtualPartitioner.deviceToVirtualStorageGroupId(d1);
    int sg2 = hashVirtualPartitioner.deviceToVirtualStorageGroupId(d2);

    assertEquals(sg1, Math.abs(d1.hashCode() % hashVirtualPartitioner.getPartitionCount()));
    assertEquals(sg2, Math.abs(d2.hashCode() % hashVirtualPartitioner.getPartitionCount()));
  }
}
