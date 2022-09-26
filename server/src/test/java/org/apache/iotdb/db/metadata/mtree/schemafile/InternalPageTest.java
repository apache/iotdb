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
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISegment;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InternalPageTest {
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
  public void initTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(1000);

    ISegment<Integer, Integer> seg =
        ISchemaPage.initInternalPage(buffer, 0, 999).getAsInternalPage();
    String[] test =
        new String[] {"abc", "key3", "key4", "key9", "key5", "key6", "key112", "key888"};

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], i);
    }
    seg.syncBuffer();
    buffer.clear();
    ISegment<Integer, Integer> seg2 = ISchemaPage.loadSchemaPage(buffer).getAsInternalPage();
    Assert.assertEquals(seg.inspect(), seg2.inspect());

    Assert.assertTrue(seg2.hasRecordKey("key5"));
    Assert.assertFalse(seg2.hasRecordKey("key51"));
    Assert.assertEquals(2, seg2.getRecordByKey("key41").intValue());
    Assert.assertEquals(0, seg2.getRecordByKey("abd").intValue());
    Assert.assertEquals(3, seg2.getRecordByKey("zzz").intValue());
  }

  @Test
  public void evenSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(170);

    ISegment<Integer, Integer> seg =
        ISchemaPage.initInternalPage(buffer, 0, 999).getAsInternalPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], i);
    }

    ByteBuffer buf2 = ByteBuffer.allocate(170);
    String sk = seg.splitByKey("a8", 666, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a5", sk);
    buf2.clear();
    ISegment<Integer, Integer> seg2 = ISchemaPage.loadSchemaPage(buf2).getAsInternalPage();
    Assert.assertEquals(4, seg2.getRecordByKey("a5").intValue());
    Assert.assertEquals(5, seg2.getRecordByKey("a6").intValue());
    Assert.assertEquals(999, seg.getRecordByKey("a").intValue());
  }

  @Test
  public void increasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(300);

    ISegment<Integer, Integer> seg =
        ISchemaPage.initInternalPage(buffer, 0, 999).getAsInternalPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], i);
    }

    seg.insertRecord("a61", 10);
    seg.insertRecord("a62", 11);
    seg.insertRecord("a63", 12);

    ByteBuffer buf2 = ByteBuffer.allocate(300);

    // split when insert the biggest key
    String sk = seg.splitByKey("a99", 666, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a9", sk);
    buf2.clear();
    Assert.assertEquals(
        Integer.valueOf(7),
        ISchemaPage.loadSchemaPage(buf2).getAsInternalPage().getRecordByKey("a91"));

    Assert.assertEquals(117, seg.insertRecord("a1", 0));

    buf2.clear();
    seg.insertRecord("a21", 20);
    seg.insertRecord("a22", 21);
    seg.insertRecord("a23", 22);

    // split when insert the second-biggest key
    sk = seg.splitByKey("a64", 6464, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a63", sk);

    seg.insertRecord("a11", 11);
    seg.insertRecord("a12", 12);

    buf2.clear();
    sk = seg.splitByKey("a24", 24, buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a23", sk);
    buf2.clear();
    Assert.assertEquals(
        Integer.valueOf(24),
        ISchemaPage.loadSchemaPage(buf2).getAsInternalPage().getRecordByKey("a24"));

    Assert.assertEquals(172, seg.insertRecord("a1", 0));
    buf2.clear();
    Assert.assertEquals(
        159, ISchemaPage.loadSchemaPage(buf2).getAsInternalPage().insertRecord("a24", 0));
  }

  @Test
  public void decreasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(300);

    ISegment<Integer, Integer> seg =
        ISchemaPage.initInternalPage(buffer, 0, 999).getAsInternalPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};

    for (int i = test.length - 1; i >= 0; i--) {
      seg.insertRecord(test[i], i);
    }

    ByteBuffer buf2 = ByteBuffer.allocate(300);

    // split with the smallest key
    String sk = seg.splitByKey("a0", 90, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a1", sk);

    Assert.assertEquals(246, seg.insertRecord("a0", 9));
    buf2.clear();
    Assert.assertEquals(
        162, ISchemaPage.loadSchemaPage(buf2).getAsInternalPage().insertRecord("a2", 0));

    seg.insertRecord("a13", 12);
    seg.insertRecord("a12", 11);

    // split with the second-smallest key
    sk = seg.splitByKey("a11", 110, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a11", sk);
    Assert.assertEquals(246, seg.insertRecord("a0", 1));
    buf2.clear();
    Assert.assertEquals(
        Integer.valueOf(110),
        ISchemaPage.loadSchemaPage(buf2).getAsInternalPage().getRecordByKey("a11"));
  }

  @Test
  public void increasingOnLowIndex() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(300);

    ISegment<Integer, Integer> seg =
        ISchemaPage.initInternalPage(buffer, 0, 999).getAsInternalPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"};

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], i);
    }

    seg.insertRecord("a01", 10);
    seg.insertRecord("a02", 20);

    ByteBuffer buf2 = ByteBuffer.allocate(300);

    // split when insert the biggest key
    String sk = seg.splitByKey("a04", 30, buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a3", sk);
    Assert.assertEquals(6, seg.getAllRecords().size());
  }

  public void print(ByteBuffer buf) throws MetadataException {
    System.out.println(ISchemaPage.loadSchemaPage(buf).getAsInternalPage().inspect());
  }

  public void print(Object s) {
    System.out.println(s);
  }
}
