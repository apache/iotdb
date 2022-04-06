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

package org.apache.iotdb.db.metadata.mtree.disk;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.estimator.BasicMNodSizeEstimator;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.CachedMNodeSizeEstimator;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IMemManager;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.MemManagerHolder;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.rescon.MemoryStatistics;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class MemManagerTest {

  private IoTDBConfig config;

  @Before
  public void setUp() throws Exception {
    config = IoTDBDescriptor.getInstance().getConfig();
    config.setSchemaEngineMode(SchemaEngineMode.Schema_File.toString());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setSchemaEngineMode(SchemaEngineMode.Memory.toString());
  }

  @Test
  public void testNodeEstimatedSizeBasedMemControl() throws Exception {
    LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;
    schemaProcessor.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());

    MemoryStatistics memoryStatistics = MemoryStatistics.getInstance();
    IMemManager memManager = MemManagerHolder.getMemManagerInstance();

    IStorageGroupMNode storageGroupMNode =
        schemaProcessor.getStorageGroupNodeByPath(new PartialPath("root.laptop"));
    IMNode deviceNode = schemaProcessor.getDeviceNode(new PartialPath("root.laptop.d1"));
    IMeasurementMNode measurementMNode =
        schemaProcessor.getMeasurementMNode(new PartialPath("root.laptop.d1.s1"));

    BasicMNodSizeEstimator basicMNodSizeEstimator = new BasicMNodSizeEstimator();
    int permSgSize = basicMNodSizeEstimator.estimateSize(storageGroupMNode);

    CachedMNodeSizeEstimator cachedMNodeSizeEstimator = new CachedMNodeSizeEstimator();
    int cachedSgSize =
        cachedMNodeSizeEstimator.estimateSize(storageGroupMNode)
            - 5; // no "root." when init the sg node of schemaRegion
    int deviceSize = cachedMNodeSizeEstimator.estimateSize(deviceNode);
    int measurementSize = cachedMNodeSizeEstimator.estimateSize(measurementMNode);

    Assert.assertEquals(
        permSgSize + cachedSgSize + deviceSize + measurementSize,
        memoryStatistics.getMemoryUsage());
    Assert.assertEquals(
        cachedSgSize + deviceSize,
        memManager.getPinnedSize()); // device is pinned and hold by mNodeCache
    Assert.assertEquals(measurementSize, memManager.getCachedSize());
  }
}
