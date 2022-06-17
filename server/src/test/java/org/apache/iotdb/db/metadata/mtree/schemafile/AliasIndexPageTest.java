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

public class AliasIndexPageTest {
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

    ISegment<String, String> seg = ISchemaPage.initAliasIndexPage(buffer, 0).getAsAliasIndexPage();
    String[] testKey =
        new String[] {"abc", "key3", "key4", "key9", "key5", "key6", "key112", "key888"};
    String[] testName =
        new String[] {
          "abc_a", "key3_a", "key4_a", "key9_a", "key5_a", "key6_a", "key112_a", "key888_a"
        };

    for (int i = 0; i < testKey.length; i++) {
      seg.insertRecord(testKey[i], testName[i]);
    }
    seg.syncBuffer();
    buffer.clear();
    ISegment<String, String> seg2 = ISchemaPage.loadSchemaPage(buffer).getAsAliasIndexPage();
    Assert.assertEquals(seg.inspect(), seg2.inspect());

    Assert.assertTrue(seg2.hasRecordKey("key5"));
    Assert.assertFalse(seg2.hasRecordKey("key51"));
    Assert.assertEquals("key4_a", seg2.getRecordByKey("key4"));
    Assert.assertEquals(null, seg2.getRecordByKey("abd"));
    Assert.assertEquals(null, seg2.getRecordByKey("zzz"));
  }

  @Test
  public void evenSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(170);

    ISegment<String, String> seg = ISchemaPage.initAliasIndexPage(buffer, 0).getAsAliasIndexPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};
    String[] name = companionName(test);

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], name[i]);
    }

    ByteBuffer buf2 = ByteBuffer.allocate(170);
    String sk = seg.splitByKey("a8", "alias", buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a5", sk);
    buf2.clear();
    ISegment<String, String> seg2 = ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage();

    Assert.assertEquals("a5_nm", seg2.getRecordByKey("a5"));
    Assert.assertEquals("a6_nm", seg2.getRecordByKey("a6"));
    Assert.assertEquals("alias", seg2.getRecordByKey("a8"));
    Assert.assertEquals("a3_nm", seg.getRecordByKey("a3"));
  }

  @Test
  public void increasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(400);

    ISegment<String, String> seg = ISchemaPage.initAliasIndexPage(buffer, 0).getAsAliasIndexPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};
    String[] name = companionName(test);
    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], name[i]);
    }

    seg.insertRecord("a61", "a61_nm");
    seg.insertRecord("a62", "a62_nm");
    seg.insertRecord("a63", "a63_nm");

    ByteBuffer buf2 = ByteBuffer.allocate(400);

    // split when insert the biggest key
    String sk = seg.splitByKey("a99", "a99_nm", buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a99", sk);
    Assert.assertEquals(
        "a99_nm", ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().getRecordByKey("a99"));

    int spare = seg.getSpareSize();
    Assert.assertEquals(spare, seg.insertRecord("a1", "a1alias"));
    spare -= 10 + "a1e".getBytes().length + "a1alias".getBytes().length;
    Assert.assertEquals(spare, seg.insertRecord("a1e", "a1alias"));

    seg.insertRecord("a21", "a21_nm");
    seg.insertRecord("a22", "a22_nm");
    seg.insertRecord("a23", "a23_nm");

    // split when insert the second-biggest key
    sk = seg.splitByKey("a64", "a64_mm", buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a64", sk);

    seg.insertRecord("a11", "a11_nm");
    seg.insertRecord("a12", "a12_nm");

    buf2.clear();
    sk = seg.splitByKey("a24", "a24_nm", buf2, SchemaFileConfig.INCLINED_SPLIT);

    Assert.assertEquals("a24", sk);
    buf2.clear();
    Assert.assertEquals(
        "a24_nm", ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().getRecordByKey("a24"));

    spare = seg.getSpareSize();
    Assert.assertEquals(spare, seg.insertRecord("a1", "a1_nm"));
    buf2.clear();
    spare = ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().getSpareSize();
    Assert.assertEquals(
        spare - 10 - "a24e".getBytes().length - "a24_nm".getBytes().length,
        ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().insertRecord("a24e", "a24_nm"));
  }

  @Test
  public void decreasingSplitTest() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(350);

    ISegment<String, String> seg = ISchemaPage.initAliasIndexPage(buffer, 0).getAsAliasIndexPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a9"};
    String[] name = companionName(test);

    for (int i = test.length - 1; i >= 0; i--) {
      seg.insertRecord(test[i], name[i]);
    }

    ByteBuffer buf2 = ByteBuffer.allocate(350);

    // split with the smallest key
    String sk = seg.splitByKey("a0", "a0_nm", buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a1", sk);

    int spare = seg.getSpareSize();
    Assert.assertEquals(spare, seg.insertRecord("a0", "a00"));
    buf2.clear();
    spare = ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().getSpareSize();
    Assert.assertEquals(
        spare, ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().insertRecord("a2", "a222"));

    seg.insertRecord("a13", "a133");
    seg.insertRecord("a12", "a122");

    // split with the second-smallest key
    sk = seg.splitByKey("a11", "a11_nm", buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a11", sk);
    spare = seg.getSpareSize();
    Assert.assertEquals(spare, seg.insertRecord("a0", "a00"));
    buf2.clear();
    Assert.assertEquals(
        "a11_nm", ISchemaPage.loadSchemaPage(buf2).getAsAliasIndexPage().getRecordByKey("a11"));
  }

  @Test
  public void increasingOnLowIndex() throws MetadataException {
    ByteBuffer buffer = ByteBuffer.allocate(300);

    ISegment<String, String> seg = ISchemaPage.initAliasIndexPage(buffer, 0).getAsAliasIndexPage();
    String[] test = new String[] {"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"};

    for (int i = 0; i < test.length; i++) {
      seg.insertRecord(test[i], test[i] + "_al");
    }

    seg.insertRecord("a01", "a0011");
    seg.insertRecord("a02", "a0022");

    ByteBuffer buf2 = ByteBuffer.allocate(300);

    // split when insert the biggest key
    String sk = seg.splitByKey("a04", "a0044", buf2, SchemaFileConfig.INCLINED_SPLIT);
    Assert.assertEquals("a3", sk);
    Assert.assertEquals(5, seg.getAllRecords().size());
  }

  private String[] companionName(String[] alias) {
    String[] res = new String[alias.length];
    for (int i = 0; i < alias.length; i++) {
      res[i] = alias[i] + "_nm";
    }
    return res;
  }

  public void print(ByteBuffer buf) throws MetadataException {
    System.out.println(ISchemaPage.loadSchemaPage(buf).getAsAliasIndexPage().inspect());
  }

  public void print(Object s) {
    System.out.println(s);
  }
}
