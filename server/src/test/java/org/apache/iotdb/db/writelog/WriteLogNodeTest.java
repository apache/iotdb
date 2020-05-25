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
package org.apache.iotdb.db.writelog;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteLogNodeTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;

  @Before
  public void setUp() throws Exception {
    enableWal = config.isEnableWal();
    config.setEnableWal(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setEnableWal(enableWal);
  }

  @Test
  public void testWriteLogAndSync() throws IOException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then reads the logs from file
    String identifier = "root.logTestDevice";

    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);

    InsertPlan bwInsertPlan = new InsertPlan(identifier, 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(50, new Path(identifier + ".s1"));

    logNode.write(bwInsertPlan);
    logNode.write(deletePlan);

    logNode.close();

    File walFile = new File(
        config.getWalFolder() + File.separator + identifier + File.separator + "wal1");
    assertTrue(walFile.exists());

    ILogReader reader = logNode.getLogReader();
    assertEquals(bwInsertPlan, reader.next());
    assertEquals(deletePlan, reader.next());
    reader.close();

    logNode.delete();
  }

  @Test
  public void testNotifyFlush() throws IOException {
    // this test writes a few logs and sync them
    // then calls notifyStartFlush() and notifyEndFlush() to delete old file
    String identifier = "root.logTestDevice";

    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);

    InsertPlan bwInsertPlan = new InsertPlan(identifier, 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(50, new Path(identifier + ".s1"));

    logNode.write(bwInsertPlan);
    logNode.notifyStartFlush();
    logNode.write(deletePlan);
    logNode.notifyStartFlush();

    ILogReader logReader = logNode.getLogReader();
    assertEquals(bwInsertPlan, logReader.next());
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertFalse(logReader.hasNext());
    logReader.close();

    logNode.delete();
  }

  @Test
  public void testSyncThreshold() throws IOException {
    // this test checks that if more logs than threshold are written, a sync will be triggered.
    int flushWalThreshold = config.getFlushWalThreshold();
    config.setFlushWalThreshold(2);

    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");

    InsertPlan bwInsertPlan = new InsertPlan("root.logTestDevice", 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan);

    File walFile = new File(
        config.getWalFolder() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    assertTrue(!walFile.exists());

    logNode.write(deletePlan);
    assertTrue(walFile.exists());

    logNode.delete();
    config.setFlushWalThreshold(flushWalThreshold);
  }

  @Test
  public void testDelete() throws IOException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then deletes the node

    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");

    InsertPlan bwInsertPlan = new InsertPlan("logTestDevice", 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan);
    logNode.write(deletePlan);

    logNode.forceSync();

    File walFile = new File(
        config.getWalFolder() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    assertTrue(walFile.exists());

    assertTrue(new File(logNode.getLogDirectory()).exists());
    logNode.delete();
    assertTrue(!new File(logNode.getLogDirectory()).exists());
  }

  @Test
  public void testOverSizedWAL() throws IOException {
    // this test uses a dummy insert log node to insert an over-sized log and assert exception caught
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice.oversize");

    InsertPlan bwInsertPlan = new InsertPlan("root.logTestDevice.oversize", 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new String[]{"1.0", "15", new String(new char[65 * 1024 * 1024]), "false"});

    boolean caught = false;
    try {
      logNode.write(bwInsertPlan);
    } catch (IOException e) {
      caught = true;
    }
    assertTrue(caught);

    logNode.delete();
  }
}
