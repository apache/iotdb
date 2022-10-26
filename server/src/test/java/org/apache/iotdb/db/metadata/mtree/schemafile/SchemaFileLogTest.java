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
package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static org.apache.iotdb.db.metadata.mtree.schemafile.SchemaFileTest.getSegAddrInContainer;
import static org.apache.iotdb.db.metadata.mtree.schemafile.SchemaFileTest.getTreeBFT;
import static org.apache.iotdb.db.metadata.mtree.schemafile.SchemaFileTest.virtualTriangleMTree;
import static org.junit.Assert.fail;

public class SchemaFileLogTest {

  private static final int TEST_SCHEMA_REGION_ID = 0;

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.Schema_File.toString());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.Memory.toString());
  }

  @Test
  public void essentialLogTest() throws IOException, MetadataException {
    SchemaFile sf =
        (SchemaFile) SchemaFile.initSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);
    IStorageGroupMNode newSGNode = new StorageGroupEntityMNode(null, "newSG", 10000L);
    sf.updateStorageGroupNode(newSGNode);

    IMNode root = virtualTriangleMTree(5, "root.test");

    Iterator<IMNode> ite = getTreeBFT(root);
    IMNode lastNode = null;
    while (ite.hasNext()) {
      IMNode curNode = ite.next();
      if (!curNode.isMeasurement()) {
        sf.writeMNode(curNode);
        lastNode = curNode;
      }
    }

    long address = getSegAddrInContainer(lastNode);
    int corruptPageIndex = SchemaFile.getPageIndex(address);

    ISchemaPage corPage =
        ISchemaPage.initSegmentedPage(
            ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), corruptPageIndex);

    // record number of children now
    Iterator<IMNode> res = sf.getChildren(lastNode);
    int cnt = 0;
    while (res.hasNext()) {
      cnt++;
      res.next();
    }

    try {
      Class schemaFileClass = SchemaFile.class;
      Field channelField = schemaFileClass.getDeclaredField("channel");
      channelField.setAccessible(true);

      FileChannel fileChannel = (FileChannel) channelField.get(sf);
      corPage.flushPageToChannel(fileChannel);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } finally {
      sf.close();
    }

    sf = (SchemaFile) SchemaFile.loadSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);
    try {
      sf.getChildren(lastNode);
      fail();
    } catch (Exception e) {
      Assert.assertEquals("Segment(index:0) not found in page(index:2).", e.getMessage());
    } finally {
      sf.close();
    }

    // modify log file to restore schema file
    FileOutputStream outputStream = null;
    FileChannel channel;
    try {
      String[] logFilePath =
          new String[] {
            "target", "tmp", "system", "schema", "root.test.vRoot1", "0", "schema_file_log.bin"
          };
      File logFile = new File(String.join(File.separator, logFilePath));
      outputStream = new FileOutputStream(logFile, true);
      channel = outputStream.getChannel();
      channel.truncate(channel.size() - 1);
    } finally {
      outputStream.close();
    }

    // verify that schema file has been repaired
    sf = (SchemaFile) SchemaFile.loadSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);
    res = sf.getChildren(lastNode);
    int cnt2 = 0;
    while (res.hasNext()) {
      res.next();
      cnt2++;
    }
    Assert.assertEquals(cnt, cnt2);
    sf.close();
  }
}
