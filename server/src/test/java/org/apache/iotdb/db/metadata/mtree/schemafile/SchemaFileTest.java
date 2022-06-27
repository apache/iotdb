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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.Segment;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class SchemaFileTest {

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
  public void essentialTestSchemaFile() throws IOException, MetadataException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);
    IStorageGroupMNode newSGNode = new StorageGroupEntityMNode(null, "newSG", 10000L);
    sf.updateStorageGroupNode(newSGNode);

    IMNode root = virtualTriangleMTree(5, "root.test");
    IMNode int0 = root.getChild("int0");
    IMNode int1 = root.getChild("int0").getChild("int1");
    IMNode int4 =
        root.getChild("int0").getChild("int1").getChild("int2").getChild("int3").getChild("int4");
    ICachedMNodeContainer.getCachedMNodeContainer(int0)
        .getNewChildBuffer()
        .put("mint1", getMeasurementNode(int0, "mint1", "alas"));

    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      IMNode curNode = ite.next();
      if (!curNode.isMeasurement()) {
        sf.writeMNode(curNode);
      }
    }
    System.out.println(((SchemaFile) sf).inspect());

    ICachedMNodeContainer.getCachedMNodeContainer(int0).getNewChildBuffer().clear();
    addNodeToUpdateBuffer(int0, getMeasurementNode(int0, "mint1", "alas99999"));

    sf.writeMNode(int0);
    Assert.assertEquals(
        "alas99999", sf.getChildNode(int0, "mint1").getAsMeasurementMNode().getAlias());

    ICachedMNodeContainer.getCachedMNodeContainer(int1).getNewChildBuffer().clear();
    ICachedMNodeContainer.getCachedMNodeContainer(int1)
        .appendMNode(getMeasurementNode(int1, "int1newM", "alas"));
    sf.writeMNode(int1);
    Assert.assertEquals(
        "alas", sf.getChildNode(int1, "int1newM").getAsMeasurementMNode().getAlias());

    ICachedMNodeContainer.getCachedMNodeContainer(int4).getNewChildBuffer().clear();
    ICachedMNodeContainer.getCachedMNodeContainer(int4)
        .getNewChildBuffer()
        .put("AAAAA", getMeasurementNode(int4, "AAAAA", "alas"));
    sf.writeMNode(int4);
    Assert.assertEquals("alas", sf.getChildNode(int4, "AAAAA").getAsMeasurementMNode().getAlias());

    ICachedMNodeContainer.getCachedMNodeContainer(int4).getUpdatedChildBuffer().clear();
    addNodeToUpdateBuffer(int4, getMeasurementNode(int4, "AAAAA", "BBBBBB"));
    sf.writeMNode(int4);
    Assert.assertEquals(
        "BBBBBB", sf.getChildNode(int4, "AAAAA").getAsMeasurementMNode().getAlias());

    ICachedMNodeContainer.getCachedMNodeContainer(int4).getUpdatedChildBuffer().clear();
    ICachedMNodeContainer.getCachedMNodeContainer(int4)
        .getUpdatedChildBuffer()
        .put("finalM191", getMeasurementNode(int4, "finalM191", "ALLLLLLLLLLLLLLLLLLLLfinalM191"));
    sf.writeMNode(int4);
    Assert.assertEquals(
        "ALLLLLLLLLLLLLLLLLLLLfinalM191",
        sf.getChildNode(int4, "finalM191").getAsMeasurementMNode().getAlias());

    sf.close();

    ISchemaFile nsf = SchemaFile.loadSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);
    Assert.assertEquals(
        "alas99999", nsf.getChildNode(int0, "mint1").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "alas", nsf.getChildNode(int1, "int1newM").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "BBBBBB", nsf.getChildNode(int4, "AAAAA").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "ALLLLLLLLLLLLLLLLLLLLfinalM191",
        nsf.getChildNode(int4, "finalM191").getAsMeasurementMNode().getAlias());
    nsf.close();
  }

  @Test
  public void testVerticalTree() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sgvt.vt", TEST_SCHEMA_REGION_ID);
    IStorageGroupMNode sgNode = new StorageGroupEntityMNode(null, "sg", 11_111L);
    sf.updateStorageGroupNode(sgNode);

    IMNode root = getVerticalTree(100, "VT");
    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      sf.writeMNode(ite.next());
    }

    IMNode vt1 = getNode(root, "root.VT_0.VT_1");
    IMNode vt4 = getNode(root, "root.VT_0.VT_1.VT_2.VT_3.VT_4");
    ICachedMNodeContainer.getCachedMNodeContainer(vt1).getNewChildBuffer().clear();
    addMeasurementChild(vt1, "newM");
    sf.writeMNode(vt1);

    IMNode vt0 = getNode(root, "root.VT_0");
    Assert.assertEquals(
        ICachedMNodeContainer.getCachedMNodeContainer(vt1).getSegmentAddress(),
        RecordUtils.getRecordSegAddr(
            getSegment(sf, ICachedMNodeContainer.getCachedMNodeContainer(vt0).getSegmentAddress())
                .getRecord("VT_1")));
    Assert.assertEquals(
        2,
        getSegment(sf, ICachedMNodeContainer.getCachedMNodeContainer(vt1).getSegmentAddress())
            .getKeyOffsetList()
            .size());
    sf.close();

    ISchemaFile nsf = SchemaFile.loadSchemaFile("root.sgvt.vt", TEST_SCHEMA_REGION_ID);

    ICachedMNodeContainer.getCachedMNodeContainer(vt1).getNewChildBuffer().clear();
    ICachedMNodeContainer.getCachedMNodeContainer(vt4).getNewChildBuffer().clear();
    Set<String> newNodes = new HashSet<>();
    for (int i = 0; i < 15; i++) {
      addMeasurementChild(vt1, "r1_" + i);
      addMeasurementChild(vt4, "r4_" + i);
      newNodes.add("r1_" + i);
      newNodes.add("r4_" + i);
    }
    nsf.writeMNode(vt1);
    nsf.writeMNode(vt4);

    nsf.close();
    nsf = SchemaFile.loadSchemaFile("root.sgvt.vt", TEST_SCHEMA_REGION_ID);

    Iterator<IMNode> vt1Children = nsf.getChildren(vt1);
    Iterator<IMNode> vt4Children = nsf.getChildren(vt4);

    while (vt1Children.hasNext()) {
      newNodes.remove(vt1Children.next().getName());
    }

    while (vt4Children.hasNext()) {
      newNodes.remove(vt4Children.next().getName());
    }

    Assert.assertTrue(newNodes.isEmpty());

    ICachedMNodeContainer.getCachedMNodeContainer(vt1).getNewChildBuffer().clear();
    ICachedMNodeContainer.getCachedMNodeContainer(vt4).getNewChildBuffer().clear();
    for (int i = 0; i < 660; i++) {
      addMeasurementChild(vt1, "2r1_" + i);
      addMeasurementChild(vt4, "2r4_" + i);
      newNodes.add("2r1_" + i);
      newNodes.add("2r4_" + i);
    }
    nsf.writeMNode(vt1);
    nsf.writeMNode(vt4);

    Assert.assertEquals(11111L, nsf.init().getAsStorageGroupMNode().getDataTTL());

    nsf.close();
  }

  @Test
  public void testFaltTree() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);

    Iterator<IMNode> ite = getTreeBFT(getFlatTree(50000, "aa"));
    while (ite.hasNext()) {
      IMNode cur = ite.next();
      if (!cur.isMeasurement()) {
        sf.writeMNode(cur);
      }
    }

    IMNode node = new InternalMNode(null, "a");
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(0L);
    List<Integer> tryReadList = Arrays.asList(199, 1999, 2999, 3999, 4999, 5999);
    for (Integer rid : tryReadList) {
      IMNode target = sf.getChildNode(node, "aa" + rid);
      Assert.assertEquals("aa" + rid + "als", target.getAsMeasurementMNode().getAlias());
    }
    sf.close();
  }

  @Test
  public void testGetChildren() throws MetadataException, IOException {
    essentialTestSchemaFile();

    IMNode node = new InternalMNode(null, "test");
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(196608L);
    ISchemaFile sf = SchemaFile.loadSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);

    Iterator<IMNode> res = sf.getChildren(node);
    int cnt = 0;
    while (res.hasNext()) {
      res.next();
      cnt++;
    }
    sf.close();
    Assert.assertEquals(1002, cnt);
  }

  @Test
  public void test10KDevices() throws MetadataException, IOException {
    int i = 1000;
    IMNode sgNode = new StorageGroupMNode(null, "sgRoot", 11111111L);

    // write with empty entitiy
    while (i >= 0) {
      IMNode aDevice = new InternalMNode(sgNode, "dev_" + i);
      sgNode.addChild(aDevice);
      i--;
    }

    Iterator<IMNode> orderedTree = getTreeBFT(sgNode);
    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    ICachedMNodeContainer.getCachedMNodeContainer(sgNode).setSegmentAddress(0L);
    IMNode node = null;
    try {
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement()) {
          sf.writeMNode(node);
        }
      }

      // update to entity
      i = 1000;
      while (i >= 0) {
        long addr = getSegAddrInContainer(sgNode.getChild("dev_" + i));
        IMNode aDevice = new EntityMNode(sgNode, "dev_" + i);
        sgNode.deleteChild(aDevice.getName());
        sgNode.addChild(aDevice);
        moveToUpdateBuffer(sgNode, "dev_" + i);
        ICachedMNodeContainer.getCachedMNodeContainer(aDevice).setSegmentAddress(addr);
        i--;
      }

      orderedTree = getTreeBFT(sgNode);
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement()) {
          sf.writeMNode(node);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(node.getName());
    } finally {
      sf.close();
    }

    // write with few measurement
    for (IMNode etn : sgNode.getChildren().values()) {
      int j = 10;
      while (j >= 0) {
        addMeasurementChild(etn, String.format("mtc_%d_%d", i, j));
        j--;
      }
    }

    orderedTree = getTreeBFT(sgNode);
    sf = SchemaFile.loadSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    try {
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement() && !node.isStorageGroup()) {
          sf.writeMNode(node);
          ICachedMNodeContainer.getCachedMNodeContainer(node).getNewChildBuffer().clear();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(node.getName());
    } finally {
      sf.close();
    }

    Set<String> resName = new HashSet<>();
    // more measurement
    for (IMNode etn : sgNode.getChildren().values()) {
      int j = 1000;
      while (j >= 0) {
        addMeasurementChild(etn, String.format("mtc2_%d_%d", i, j));
        if (resName.size() < 101) {
          resName.add(String.format("mtc2_%d_%d", i, j));
        }
        j--;
      }
    }

    orderedTree = getTreeBFT(sgNode);
    sf = SchemaFile.loadSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    List<IMNode> arbitraryNode = new ArrayList<>();
    try {
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement() && !node.isStorageGroup()) {
          sf.writeMNode(node);
          if (arbitraryNode.size() < 50) {
            arbitraryNode.add(node);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(node.getName());
    } finally {
      sf.close();
    }

    sf = SchemaFile.loadSchemaFile("sgRoot", TEST_SCHEMA_REGION_ID);

    for (String key : resName) {
      IMNode resNode = sf.getChildNode(arbitraryNode.get(arbitraryNode.size() - 3), key);
      Assert.assertTrue(
          resNode.getAsMeasurementMNode().getAlias().equals(resNode.getName() + "alias"));
    }

    Iterator<IMNode> res = sf.getChildren(arbitraryNode.get(arbitraryNode.size() - 1));
    int i2 = 0;
    while (res.hasNext()) {
      resName.remove(res.next().getName());
    }

    Assert.assertTrue(resName.isEmpty());
    sf.close();
  }

  @Test
  public void testUpdateOnFullPageSegment() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    IMNode root = getFlatTree(783, "aa");
    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      IMNode cur = ite.next();
      if (!cur.isMeasurement()) {
        sf.writeMNode(cur);
      }
    }

    root.getChildren().clear();
    root.addChild(getMeasurementNode(root, "aa0", "updatedupdatednode"));

    ICachedMNodeContainer.getCachedMNodeContainer(root).moveMNodeToCache("aa0");
    ICachedMNodeContainer.getCachedMNodeContainer(root).updateMNode("aa0");

    sf.writeMNode(root);

    Assert.assertEquals(
        "updatedupdatednode", sf.getChildNode(root, "aa0").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        1,
        getSegment(sf, getSegAddr(sf, getSegAddrInContainer(root), "aa0"))
            .getKeyOffsetList()
            .size());

    root.getChildren().clear();

    root.addChild(new EntityMNode(root, "ent1"));

    IMNode ent1 = root.getChild("ent1");
    ent1.addChild(getMeasurementNode(ent1, "m1", "m1a"));

    sf.writeMNode(root);
    sf.writeMNode(ent1);

    ent1.getChildren().clear();

    ent1.addChild(getMeasurementNode(ent1, "m1", "m1aaaaaa"));
    ICachedMNodeContainer.getCachedMNodeContainer(ent1).moveMNodeToCache("m1");
    ICachedMNodeContainer.getCachedMNodeContainer(ent1).updateMNode("m1");

    Assert.assertEquals(
        64, getSegment(sf, getSegAddr(sf, getSegAddrInContainer(ent1), "m1")).size());

    sf.writeMNode(ent1);

    Assert.assertEquals(
        1024, getSegment(sf, getSegAddr(sf, getSegAddrInContainer(ent1), "m1")).size());

    ent1.getChildren().clear();

    while (ent1.getChildren().size() < 374) {
      addMeasurementChild(ent1, "nc" + ent1.getChildren().size());
    }

    sf.writeMNode(ent1);
    ent1.getChildren().clear();
    ent1.addChild(getMeasurementNode(ent1, "nc0", "updated_nc0updated_nc0updated_nc0updated_nc0"));
    moveToUpdateBuffer(ent1, "nc0");
    sf.writeMNode(ent1);

    ent1.getChildren().clear();
    ent1.addChild(getMeasurementNode(ent1, "nc1", "updated_nc1updated_nc1updated_nc1updated_nc1"));
    moveToUpdateBuffer(ent1, "nc1");
    sf.writeMNode(ent1);

    Assert.assertEquals(
        getSegAddr(sf, getSegAddrInContainer(ent1), "nc1"),
        getSegAddr(sf, getSegAddrInContainer(ent1), "nc0"));

    sf.close();
  }

  @Test
  public void testRearrangementWhenInsert() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    IMNode root = new StorageGroupEntityMNode(null, "sgRoot", 0L);

    root.getChildren().clear();
    IMNode ent2 = new EntityMNode(root, "ent2");
    IMNode ent3 = new EntityMNode(root, "ent3");
    IMNode ent4 = new EntityMNode(root, "ent4");
    root.addChild(ent2);
    root.addChild(ent3);
    root.addChild(ent4);

    while (ent4.getChildren().size() < 19) {
      ent4.addChild(
          getMeasurementNode(
              ent4, "e4m" + ent4.getChildren().size(), "e4malais" + ent4.getChildren().size()));
    }
    sf.writeMNode(root);
    sf.writeMNode(ent4);

    ent4.getChildren().clear();
    ent4.addChild(
        getMeasurementNode(
            ent4,
            "e4m0",
            "updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_updated_"));
    moveToUpdateBuffer(ent4, "e4m0");
    sf.writeMNode(ent4);

    while (ent2.getChildren().size() < 19) {
      ent2.addChild(
          getMeasurementNode(
              ent2, "e2m" + ent2.getChildren().size(), "e2malais" + ent2.getChildren().size()));
    }
    sf.writeMNode(ent2);

    while (ent3.getChildren().size() < 180) {
      ent3.addChild(
          getMeasurementNode(
              ent3, "e3m" + ent3.getChildren().size(), "e3malais" + ent3.getChildren().size()));
    }
    sf.writeMNode(ent3);

    ent2.getChildren().clear();
    while (ent2.getChildren().size() < 70) {
      ent2.addChild(
          getMeasurementNode(
              ent2, "e2ms" + ent2.getChildren().size(), "e2is_s2_" + ent2.getChildren().size()));
    }
    sf.writeMNode(ent2);

    Assert.assertEquals(
        getSegAddr(sf, getSegAddrInContainer(ent2), "e2m0") - 1,
        getSegAddr(sf, getSegAddrInContainer(ent3), "e3m0"));
    Assert.assertEquals(
        getSegAddr(sf, getSegAddrInContainer(ent2), "e2m0") - 2,
        getSegAddr(sf, getSegAddrInContainer(ent4), "e4m0"));

    root.getChildren().clear();
    IMNode ent5 = new EntityMNode(root, "ent5");
    root.addChild(ent5);
    while (ent5.getChildren().size() < 19) {
      ent5.addChild(
          getMeasurementNode(
              ent5,
              "e5mk" + ent5.getChildren().size(),
              "e5malaikkkkks" + ent5.getChildren().size()));
    }

    sf.writeMNode(root);
    sf.writeMNode(ent5);
    ent5.getChildren().clear();
    ent5.addChild(
        getMeasurementNode(
            ent5,
            "e5extm",
            "e5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5"
                + "malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkks"));
    sf.writeMNode(ent5);
    Assert.assertEquals(20, getSegment(sf, getSegAddrInContainer(ent5)).getAllRecords().size());

    ent5.getChildren().clear();
    addNodeToUpdateBuffer(ent5, getMeasurementNode(ent5, "e5extm", null));
    sf.writeMNode(ent5);

    Assert.assertEquals(null, sf.getChildNode(ent5, "e5extm").getAsMeasurementMNode().getAlias());

    sf.close();
  }

  @Test
  public void bitwiseTest() {
    long initGlbAdr = 1099780063232L;
    int pageIndex = SchemaFile.getPageIndex(initGlbAdr);

    int bs = 1;
    while (bs <= 32) {
      long highBits = 0xffffffff00000000L & ((0xffffffffL & pageIndex) << 1);
      pageIndex <<= 1;
      pageIndex |= highBits >>> 32;
      bs++;
      Assert.assertEquals(
          pageIndex, SchemaFile.getPageIndex(SchemaFile.getGlobalIndex(pageIndex, (short) 0)));
    }

    short segIdx = SchemaFile.getSegIndex(initGlbAdr);
    while (initGlbAdr < 1099980063232L) {
      Assert.assertEquals(initGlbAdr, SchemaFile.getGlobalIndex(pageIndex, segIdx));
      initGlbAdr += 0x80000000L;
      pageIndex = SchemaFile.getPageIndex(initGlbAdr);
      segIdx = SchemaFile.getSegIndex(initGlbAdr);
    }
  }

  // region Quick Print

  private void printSF(ISchemaFile file) throws IOException, MetadataException {
    System.out.println(((SchemaFile) file).inspect());
  }

  public static void print(Object o) {
    System.out.println(o.toString());
  }

  // endregion

  // region Schema File Shortcut
  private SchemaPage getPage(ISchemaFile sf, long addr) {
    try {
      return ((SchemaFile) sf).getPageOnTest(SchemaFile.getPageIndex(addr));
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static Segment getSegment(ISchemaFile sf, long address) {
    try {
      return ((SchemaFile) sf)
          .getPageOnTest(SchemaFile.getPageIndex(address))
          .getSegmentTest(SchemaFile.getSegIndex(address));
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static long getSegAddr(ISchemaFile sf, long curAddr, String key) {
    try {
      return ((SchemaFile) sf).getTargetSegmentOnTest(curAddr, key);
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
      return -1L;
    }
  }

  // endregion

  // region IMNode Shortcut
  private void addMeasurementChild(IMNode par, String mid) {
    par.addChild(getMeasurementNode(par, mid, mid + "alias"));
  }

  private IMeasurementSchema getSchema(String id) {
    return new MeasurementSchema(id, TSDataType.FLOAT);
  }

  private IMNode getNode(IMNode root, String path) throws MetadataException {
    String[] pathNodes = PathUtils.splitPathToDetachedNodes(path);
    IMNode cur = root;
    for (String node : pathNodes) {
      if (!node.equals("root")) {
        cur = cur.getChild(node);
      }
    }
    return cur;
  }

  private IMNode getInternalWithSegAddr(IMNode par, String name, long segAddr) {
    IMNode node = new EntityMNode(par, name);
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(segAddr);
    return node;
  }

  private IMNode getMeasurementNode(IMNode par, String name, String alias) {
    IMeasurementSchema schema = new MeasurementSchema(name, TSDataType.FLOAT);
    IMeasurementMNode mNode =
        MeasurementMNode.getMeasurementMNode(par.getAsEntityMNode(), name, schema, alias);
    return mNode;
  }

  private static void addNodeToUpdateBuffer(IMNode par, IMNode child) {
    ICachedMNodeContainer.getCachedMNodeContainer(par).remove(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).appendMNode(child);
    ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(child.getName());
  }

  private static void moveToUpdateBuffer(IMNode par, String childName) {
    ICachedMNodeContainer.getCachedMNodeContainer(par).appendMNode(par.getChild(childName));
    ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(childName);
    ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(childName);
  }

  private static long getSegAddrInContainer(IMNode par) {
    return ICachedMNodeContainer.getCachedMNodeContainer(par).getSegmentAddress();
  }

  // endregion

  // region Tree Constructor

  private IMNode virtualTriangleMTree(int size, String sgPath) throws MetadataException {
    String[] sgPathNodes = PathUtils.splitPathToDetachedNodes(sgPath);
    IMNode upperNode = null;
    for (String name : sgPathNodes) {
      IMNode child = new InternalMNode(upperNode, name);
      upperNode = child;
    }
    IMNode internalNode = new StorageGroupEntityMNode(upperNode, "vRoot1", 0L);

    for (int idx = 0; idx < size; idx++) {
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }

    IMNode curNode = internalNode;
    for (int idx = 0; idx < size; idx++) {
      String nodeName = "int" + idx;
      IMNode newNode = new EntityMNode(curNode, nodeName);
      curNode.addChild(newNode);
      curNode = newNode;
    }

    for (int idx = 0; idx < 1000; idx++) {
      IMeasurementSchema schema = new MeasurementSchema("finalM" + idx, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), "finalM" + idx, schema, "finalals" + idx);
      curNode.addChild(mNode);
    }
    IMeasurementSchema schema = new MeasurementSchema("finalM", TSDataType.FLOAT);
    IMeasurementMNode mNode =
        MeasurementMNode.getMeasurementMNode(
            internalNode.getAsEntityMNode(), "finalM", schema, "finalals");
    curNode.addChild(mNode);
    upperNode.addChild(internalNode);
    return internalNode;
  }

  private IMNode getFlatTree(int flatSize, String id) {
    IMNode root = new InternalMNode(null, "root");
    IMNode test = new InternalMNode(root, "test");
    IMNode internalNode = new StorageGroupEntityMNode(null, "vRoot1", 0L);

    for (int idx = 0; idx < flatSize; idx++) {
      String measurementId = id + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }

    test.addChild(internalNode);
    return internalNode;
  }

  private IMNode getVerticalTree(int height, String id) {
    IMNode trueRoot = new InternalMNode(null, "root");
    trueRoot.addChild(new InternalMNode(trueRoot, "sgvt"));
    IMNode root = new StorageGroupEntityMNode(null, "vt", 0L);
    int cnt = 0;
    IMNode cur = root;
    while (cnt < height) {
      cur.addChild(new EntityMNode(cur, id + "_" + cnt));
      cur = cur.getChild(id + "_" + cnt);
      cnt++;
    }
    trueRoot.getChild("sgvt").addChild(root);
    return root;
  }

  private Iterator<IMNode> getTreeBFT(IMNode root) {
    return new Iterator<IMNode>() {
      Queue<IMNode> queue = new LinkedList<IMNode>();

      {
        this.queue.add(root);
      }

      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public IMNode next() {
        IMNode curNode = queue.poll();
        if (!curNode.isMeasurement() && curNode.getChildren().size() > 0) {
          for (IMNode child : curNode.getChildren().values()) {
            queue.add(child);
          }
        }
        return curNode;
      }
    };
  }

  // endregion

}
