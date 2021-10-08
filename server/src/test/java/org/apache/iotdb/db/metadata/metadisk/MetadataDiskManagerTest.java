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
package org.apache.iotdb.db.metadata.metadisk;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MetadataDiskManagerTest {

  private static final int CACHE_SIZE = 10;
  private static final String BASE_PATH = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
  private static final String METAFILE_FILEPATH =
      BASE_PATH + "MetadataDiskManagerTest_metafile.bin";
  private String SNAPSHOT_PATH = METAFILE_FILEPATH + ".snapshot.bin";
  private String SNAPSHOT_TEMP_PATH = SNAPSHOT_PATH + ".tmp";

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    File file = new File(METAFILE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    file = new File(SNAPSHOT_PATH);
    if (file.exists()) {
      file.delete();
    }
    file = new File(SNAPSHOT_TEMP_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(METAFILE_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    file = new File(SNAPSHOT_PATH);
    if (file.exists()) {
      file.delete();
    }
    file = new File(SNAPSHOT_TEMP_PATH);
    if (file.exists()) {
      file.delete();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRecover() throws Exception {
    MetadataDiskManager manager = new MetadataDiskManager(0, METAFILE_FILEPATH);
    IMNode root = manager.getRoot();
    IMNode sg = new StorageGroupMNode(null, "sg", 0);
    manager.addChild(root, "sg", sg);
    IMNode device = new InternalMNode(null, "device");
    manager.addChild(sg, "device", device);
    IMNode measurement = new MeasurementMNode(null, "t1", new MeasurementSchema(), null);
    manager.addChild(device, "t1", measurement);
    manager.createSnapshot();
    manager.clear();

    MetadataDiskManager recoverManager = new MetadataDiskManager(4, METAFILE_FILEPATH);
    root = recoverManager.getRoot();
    sg = recoverManager.getChild(root, "sg");
    device = recoverManager.getChild(sg, "device");
    measurement = recoverManager.getChild(device, "t1");
    Assert.assertEquals("root", root.getName());
    Assert.assertEquals("sg", sg.getName());
    Assert.assertTrue(sg.isStorageGroup());
    Assert.assertEquals("device", device.getName());
    Assert.assertEquals("t1", measurement.getName());
    Assert.assertTrue(measurement.isMeasurement());
    recoverManager.clear();
  }
}
