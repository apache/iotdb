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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class LocalConfigNodeMultiDataRegionTest {
  int originDataRegionNum;
  boolean isMppMode = false;
  boolean isClusterMode = false;

  @Before
  public void setUp() throws IllegalPathException {
    originDataRegionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
    isMppMode = IoTDBDescriptor.getInstance().getConfig().isMppMode();
    isClusterMode = IoTDBDescriptor.getInstance().getConfig().isClusterMode();
    IoTDBDescriptor.getInstance().getConfig().setMppMode(true);
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(false);
    IoTDB.configManager.init();
    EnvironmentUtils.envSetUp();
    LocalDataPartitionInfo.getInstance().init(Collections.EMPTY_MAP);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(originDataRegionNum);
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setMppMode(isMppMode);
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(isClusterMode);
  }

  @Test
  public void createMultiDataRegionTest() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(3);
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath("root.test"));
    LocalConfigNode configNode = LocalConfigNode.getInstance();
    LocalDataPartitionInfo info = LocalDataPartitionInfo.getInstance();
    info.registerStorageGroup(new PartialPath("root.test"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d1"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d2"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d3"));
    List<DataRegionId> regionIds =
        info.getDataRegionIdsByStorageGroup(new PartialPath("root.test"));
    Assert.assertEquals(3, regionIds.size());
  }

  @Test
  public void recoverMultiDataRegionTest() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(3);
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath("root.test"));
    LocalConfigNode configNode = LocalConfigNode.getInstance();
    LocalDataPartitionInfo info = LocalDataPartitionInfo.getInstance();
    info.registerStorageGroup(new PartialPath("root.test"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d1"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d2"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d3"));
    LocalConfigNode.getInstance().clear();
    LocalConfigNode.getInstance().init();
    info = LocalDataPartitionInfo.getInstance();
    info.registerStorageGroup(new PartialPath("root.test"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d1"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d2"));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath("root.test.d3"));
    List<DataRegionId> regionIds =
        info.getDataRegionIdsByStorageGroup(new PartialPath("root.test"));
    Assert.assertEquals(3, regionIds.size());
  }
}
