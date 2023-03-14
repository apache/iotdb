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
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.schemafile.ICacheMNode;
import org.apache.iotdb.db.metadata.mnode.schemafile.container.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.schemafile.factory.CacheMNodeFactory;
import org.apache.iotdb.db.metadata.mnode.utils.MNodeUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.WrappedSegment;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class SchemaFileTest {

  private static final int TEST_SCHEMA_REGION_ID = 0;
  private static final IMNodeFactory<ICacheMNode> nodeFactory = CacheMNodeFactory.getInstance();

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
    IDatabaseMNode<ICacheMNode> newSGNode =
        nodeFactory.createDatabaseDeviceMNode(null, "newSG", 10000L).getAsDatabaseMNode();
    sf.updateDatabaseNode(newSGNode);

    ICacheMNode root = virtualTriangleMTree(5, "root.test");
    ICacheMNode int0 = root.getChild("int0");
    ICacheMNode int1 = root.getChild("int0").getChild("int1");
    ICacheMNode int4 =
        root.getChild("int0").getChild("int1").getChild("int2").getChild("int3").getChild("int4");
    ICachedMNodeContainer.getCachedMNodeContainer(int0)
        .getNewChildBuffer()
        .put("mint1", getMeasurementNode(int0, "mint1", "alas"));

    Iterator<ICacheMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      ICacheMNode curNode = ite.next();
      if (!curNode.isMeasurement()) {
        sf.writeMNode(curNode);
      }
    }

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
    IDatabaseMNode<ICacheMNode> sgNode =
        nodeFactory.createDatabaseDeviceMNode(null, "sg", 11_111L).getAsDatabaseMNode();
    sf.updateDatabaseNode(sgNode);

    ICacheMNode root = getVerticalTree(100, "VT");
    Iterator<ICacheMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      sf.writeMNode(ite.next());
    }

    ICacheMNode vt1 = getNode(root, "root.VT_0.VT_1");
    ICacheMNode vt4 = getNode(root, "root.VT_0.VT_1.VT_2.VT_3.VT_4");
    ICachedMNodeContainer.getCachedMNodeContainer(vt1).getNewChildBuffer().clear();
    addMeasurementChild(vt1, "newM");
    sf.writeMNode(vt1);

    ICacheMNode vt0 = getNode(root, "root.VT_0");
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

    Iterator<ICacheMNode> vt1Children = nsf.getChildren(vt1);
    Iterator<ICacheMNode> vt4Children = nsf.getChildren(vt4);

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

    Assert.assertEquals(11111L, nsf.init().getAsDatabaseMNode().getDataTTL());

    nsf.close();
  }

  @Test
  public void testFaltTree() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);

    Iterator<ICacheMNode> ite = getTreeBFT(getFlatTree(50000, "aa"));
    while (ite.hasNext()) {
      ICacheMNode cur = ite.next();
      if (!cur.isMeasurement()) {
        sf.writeMNode(cur);
      }
    }

    ICacheMNode node = nodeFactory.createInternalMNode(null, "a");
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(0L);
    List<Integer> tryReadList = Arrays.asList(199, 1999, 2999, 3999, 4999, 5999);
    for (Integer rid : tryReadList) {
      ICacheMNode target = sf.getChildNode(node, "aa" + rid);
      Assert.assertEquals("aa" + rid + "als", target.getAsMeasurementMNode().getAlias());
    }
    sf.close();
  }

  @Test
  public void testGetChildren() throws MetadataException, IOException {
    essentialTestSchemaFile();

    ICacheMNode node = nodeFactory.createInternalMNode(null, "test");
    ICachedMNodeContainer.getCachedMNodeContainer(node)
        .setSegmentAddress(SchemaFile.getGlobalIndex(2, (short) 0));
    ISchemaFile sf = SchemaFile.loadSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);

    Iterator<ICacheMNode> res = sf.getChildren(node);
    int cnt = 0;
    while (res.hasNext()) {
      res.next();
      cnt++;
    }
    sf.close();
    Assert.assertEquals(1002, cnt);
  }

  @Test
  public void test200KMeasurement() throws MetadataException, IOException {
    int i = 200000, j = 20;
    ICacheMNode sgNode = nodeFactory.createDatabaseMNode(null, "sgRoot", 11111111L).getAsMNode();
    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);

    while (j >= 0) {
      ICacheMNode aDevice = nodeFactory.createDeviceMNode(sgNode, "dev_" + j).getAsMNode();
      sgNode.addChild(aDevice);
      j--;
    }

    sf.writeMNode(sgNode);

    ICacheMNode meas;
    ICacheMNode dev = sgNode.getChildren().get("dev_2");
    while (i >= 0) {
      meas = getMeasurementNode(dev, "m_" + i, "ma_" + i);
      dev.addChild(meas);
      i--;
    }

    sf.writeMNode(dev);

    Assert.assertEquals(
        "ma_199406", sf.getChildNode(dev, "m_199406").getAsMeasurementMNode().getAlias());
    Assert.assertEquals("m_1995", sf.getChildNode(dev, "ma_1995").getName());

    sf.delete(dev);
    Assert.assertNull(sf.getChildNode(sgNode, "dev_2"));
    sf.close();
  }

  @Test
  public void test10KDevices() throws MetadataException, IOException {
    int i = 1000;
    ICacheMNode sgNode = nodeFactory.createDatabaseMNode(null, "sgRoot", 11111111L).getAsMNode();

    // write with empty entitiy
    while (i >= 0) {
      ICacheMNode aDevice = nodeFactory.createInternalMNode(sgNode, "dev_" + i);
      sgNode.addChild(aDevice);
      i--;
    }

    Iterator<ICacheMNode> orderedTree = getTreeBFT(sgNode);
    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    ICachedMNodeContainer.getCachedMNodeContainer(sgNode).setSegmentAddress(0L);
    ICacheMNode node = null;
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
        ICacheMNode aDevice = nodeFactory.createDeviceMNode(sgNode, "dev_" + i).getAsMNode();
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
    for (ICacheMNode etn : sgNode.getChildren().values()) {
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
        if (!node.isMeasurement() && !node.isDatabase()) {
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
    for (ICacheMNode etn : sgNode.getChildren().values()) {
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
    List<ICacheMNode> arbitraryNode = new ArrayList<>();
    try {
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement() && !node.isDatabase()) {
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
      ICacheMNode resNode = sf.getChildNode(arbitraryNode.get(arbitraryNode.size() - 3), key);
      Assert.assertTrue(
          resNode.getAsMeasurementMNode().getAlias().equals(resNode.getName() + "alias"));
    }

    Iterator<ICacheMNode> res = sf.getChildren(arbitraryNode.get(arbitraryNode.size() - 1));
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
    ICacheMNode root = getFlatTree(783, "aa");
    Iterator<ICacheMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      ICacheMNode cur = ite.next();
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
    Assert.assertEquals("aa0", sf.getChildNode(root, "updatedupdatednode").getName());

    root.getChildren().clear();

    root.addChild(nodeFactory.createDeviceMNode(root, "ent1").getAsMNode());

    ICacheMNode ent1 = root.getChild("ent1");
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
        1020, getSegment(sf, getSegAddr(sf, getSegAddrInContainer(ent1), "m1")).size());

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
  public void testEstimateSegSize() throws Exception {
    // to test whether estimation of segment size works on edge cases
    /**
     * related methods shall be merged further: {@linkplain SchemaFile#reEstimateSegSize}
     * ,{@linkplain PageManager#reEstimateSegSize}
     */
    ICacheMNode sgNode = nodeFactory.createDatabaseMNode(null, "mma", 111111111L).getAsMNode();
    ICacheMNode d1 = fillChildren(sgNode, 300, "d", this::supplyEntity);
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    try {
      sf.writeMNode(sgNode);

      fillChildren(d1, 46, "s", this::supplyMeasurement);
      sf.writeMNode(d1);

      moveAllToBuffer(d1);
      moveAllToBuffer(sgNode);

      // it's an edge case where a wrapped segment need to extend to another page while its expected
      // size
      // measured by insertion batch and existed size at same time.
      fillChildren(sgNode, 350, "sd", this::supplyEntity);
      sf.writeMNode(sgNode);
      fillChildren(d1, 20, "ss", this::supplyMeasurement);
      sf.writeMNode(d1);

      Iterator<ICacheMNode> verifyChildren = sf.getChildren(d1);
      int cnt = 0;
      while (verifyChildren.hasNext()) {
        cnt++;
        verifyChildren.next();
      }
      Assert.assertEquals(66, cnt);
    } finally {
      sf.close();
    }
  }

  @Test
  public void test200KAlias() throws Exception {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    ICacheMNode sgNode = nodeFactory.createDatabaseMNode(null, "mma", 111111111L).getAsMNode();
    // 5 devices, each for 200k measurements
    int factor20K = 20000;
    List<ICacheMNode> devs = new ArrayList<>();
    List<List> senList = new ArrayList<>();
    Map<String, String> aliasAns = new HashMap<>();

    try {
      for (int i = 0; i < 5; i++) {
        devs.add(nodeFactory.createDeviceMNode(sgNode, "d_" + i).getAsMNode());
        sgNode.addChild(devs.get(i));
      }

      for (ICacheMNode dev : devs) {
        List<ICacheMNode> sens = new ArrayList<>();
        for (int i = 0; i < factor20K; i++) {
          sens.add(getMeasurementNode(dev, "s_" + i, null));
          dev.addChild(sens.get(i));

          if (dev.getName().equals("d_0")) {
            aliasAns.put("s_" + i, "als_" + i);
          }
        }
        senList.add(sens);
      }

      Iterator<ICacheMNode> ite = getTreeBFT(sgNode);

      ICacheMNode curNode;
      while (ite.hasNext()) {
        curNode = ite.next();
        if (!curNode.isMeasurement()) {
          sf.writeMNode(curNode);
        }
      }
    } finally {
      sf.sync();
      sf.close();
    }

    sf = SchemaFile.loadSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    try {
      ICacheMNode dev2 = devs.get(2);
      for (ICacheMNode child : dev2.getChildren().values()) {
        child.getAsMeasurementMNode().setAlias(aliasAns.get(child.getName()));
      }

      for (String name : aliasAns.keySet()) {
        moveToUpdateBuffer(dev2, name);
      }

      sf.writeMNode(dev2);
      sf.sync();
      sf.close();

      sf = SchemaFile.loadSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);

      for (Map.Entry<String, String> entry : aliasAns.entrySet()) {
        Assert.assertEquals(entry.getKey(), sf.getChildNode(dev2, entry.getValue()).getName());
      }

      Iterator<ICacheMNode> children = sf.getChildren(dev2);
      int cnt = 0;
      while (children.hasNext()) {
        cnt++;
        children.next();
      }
      Assert.assertEquals(factor20K, cnt);

    } finally {
      sf.close();
    }
  }

  @Test
  public void testRearrangementWhenInsert() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sg", TEST_SCHEMA_REGION_ID);
    ICacheMNode root = nodeFactory.createDatabaseDeviceMNode(null, "sgRoot", 0L);

    root.getChildren().clear();
    ICacheMNode ent2 = nodeFactory.createDeviceMNode(root, "ent2").getAsMNode();
    ICacheMNode ent3 = nodeFactory.createDeviceMNode(root, "ent3").getAsMNode();
    ICacheMNode ent4 = nodeFactory.createDeviceMNode(root, "ent4").getAsMNode();
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
        getSegAddr(sf, getSegAddrInContainer(ent2), "e2m0") + 65536,
        getSegAddr(sf, getSegAddrInContainer(ent3), "e3m0"));
    Assert.assertEquals(
        getSegAddr(sf, getSegAddrInContainer(ent2), "e2m0") + 2,
        getSegAddr(sf, getSegAddrInContainer(ent4), "e4m0"));

    root.getChildren().clear();
    ICacheMNode ent5 = nodeFactory.createDeviceMNode(root, "ent5").getAsMNode();
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
    Assert.assertEquals(
        "e5extm",
        sf.getChildNode(
                ent5,
                "e5malaikkkkkse5malaikkkkkse5malaikkkkkse5ma"
                    + "laikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkkse5malaikkkkks")
            .getName());

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

  // region B+ Tree Segment SchemaFile Test

  @Test
  public void basicTest() throws IOException, MetadataException {
    SchemaFileConfig.INTERNAL_SPLIT_VALVE = 16000;
    int i = 10000;
    ICacheMNode sgNode = nodeFactory.createDatabaseDeviceMNode(null, "sgRoot", 11111111L);
    Set<String> checkSet = new HashSet<>();
    // write with empty entitiy
    while (i >= 0) {
      String name = Integer.toString(i);
      if (i < 10) {
        name = "00" + name;
      } else if (i < 100) {
        name = "0" + name;
      }
      ICacheMNode aMeas = getMeasurementNode(sgNode, "s_" + name, null);
      checkSet.add(aMeas.getName());
      sgNode.addChild(aMeas);
      i--;
    }

    Iterator<ICacheMNode> orderedTree = getTreeBFT(sgNode);
    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    sf.writeMNode(sgNode);

    Iterator<ICacheMNode> res = sf.getChildren(sgNode);
    while (res.hasNext()) {
      checkSet.remove(res.next().getName());
    }
    Assert.assertTrue(checkSet.isEmpty());
    sf.close();
    SchemaFileConfig.INTERNAL_SPLIT_VALVE = 0;
  }

  @Test
  public void basicSplitTest() throws MetadataException, IOException {
    SchemaFileConfig.INTERNAL_SPLIT_VALVE = 16230;
    SchemaFileConfig.DETAIL_SKETCH = true;
    int i = 999;
    ICacheMNode sgNode = nodeFactory.createDatabaseDeviceMNode(null, "sgRoot", 11111111L);
    Set<String> checkSet = new HashSet<>();
    // write with empty entitiy
    while (i >= 0) {
      String name = Integer.toString(i);
      if (i < 10) {
        name = "00" + name;
      } else if (i < 100) {
        name = "0" + name;
      }
      ICacheMNode aMeas = getMeasurementNode(sgNode, "s_" + name, null);
      checkSet.add(aMeas.getName());
      sgNode.addChild(aMeas);
      i--;
    }

    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName(), TEST_SCHEMA_REGION_ID);
    sf.writeMNode(sgNode);

    Iterator<ICacheMNode> res = sf.getChildren(sgNode);

    while (res.hasNext()) {
      checkSet.remove(res.next().getName());
    }
    Assert.assertTrue(checkSet.isEmpty());

    sgNode.getChildren().clear();

    for (int j = 50; j >= 0; j--) {
      String name = Integer.toString(j);
      if (j < 10) {
        name = "00" + name;
      } else if (j < 100) {
        name = "0" + name;
      }
      ICacheMNode aMeas = nodeFactory.createInternalMNode(sgNode, "d_" + name);
      sgNode.addChild(aMeas);
    }

    for (int j = 560; j >= 0; j--) {
      String name = Integer.toString(j);
      if (j < 10) {
        name = "00" + name;
      } else if (j < 100) {
        name = "0" + name;
      }
      ICacheMNode aMeas = nodeFactory.createInternalMNode(sgNode, "dd2_" + name);
      checkSet.add(aMeas.getName());
      sgNode.getChildren().get("d_010").addChild(aMeas);
    }

    ICacheMNode d010 = sgNode.getChildren().get("d_010");
    d010 = MNodeUtils.setToEntity(d010, nodeFactory).getAsMNode();
    ICacheMNode ano = getMeasurementNode(d010, "splitover", "aliaslasialsai");

    d010.addChild(ano);
    sgNode.addChild(d010);

    sf.writeMNode(sgNode);
    sf.writeMNode(sgNode.getChildren().get("d_010"));

    ano.getAsMeasurementMNode().setAlias("aliaslasialsaialiaslasialsai");
    d010.getChildren().clear();
    d010.addChild(ano);

    moveToUpdateBuffer(d010, "splitover");
    sf.writeMNode(d010);

    int d010cs = 0;
    Iterator<ICacheMNode> res2 = sf.getChildren(d010);
    while (res2.hasNext()) {
      checkSet.add(res2.next().getName());
      d010cs++;
    }

    sf.close();

    ISchemaFile sf2 = SchemaFile.loadSchemaFile("sgRoot", TEST_SCHEMA_REGION_ID);
    res2 = sf2.getChildren(d010);
    while (res2.hasNext()) {
      checkSet.remove(res2.next().getName());
      d010cs--;
    }

    Assert.assertEquals(
        "aliaslasialsaialiaslasialsai",
        sf2.getChildNode(d010, "splitover").getAsMeasurementMNode().getAlias());

    Assert.assertEquals(
        "splitover", sf2.getChildNode(d010, "aliaslasialsaialiaslasialsai").getName());

    Assert.assertEquals(0, d010cs);
    Assert.assertTrue(checkSet.isEmpty());
    sf2.close();
    SchemaFileConfig.INTERNAL_SPLIT_VALVE = 0;
  }

  // endregion

  // region Quick Print

  private void printSF(ISchemaFile file) throws IOException, MetadataException {
    ((SchemaFile) file).inspect();
  }

  public static void print(Object o) {
    System.out.println(o.toString());
  }

  // endregion

  // region Schema File Shortcut
  private static WrappedSegment getSegment(ISchemaFile sf, long address) {
    try {
      return ((SchemaFile) sf)
          .getPageOnTest(SchemaFile.getPageIndex(address))
          .getSegmentOnTest(SchemaFile.getSegIndex(address));
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

  // region ICacheMNode Shortcut

  private ICacheMNode supplyMeasurement(ICacheMNode par, String name) {
    return getMeasurementNode(par, name, name + "_als");
  }

  private ICacheMNode supplyInternal(ICacheMNode par, String name) {
    return nodeFactory.createInternalMNode(par, name);
  }

  private ICacheMNode supplyEntity(ICacheMNode par, String name) {
    return nodeFactory.createDeviceMNode(par, name).getAsMNode();
  }

  private ICacheMNode fillChildren(
      ICacheMNode par,
      int number,
      String prefix,
      BiFunction<ICacheMNode, String, ICacheMNode> nodeFactory) {
    String childName;
    ICacheMNode lastChild = null;
    for (int i = 0; i < number; i++) {
      childName = prefix + "_" + i;
      lastChild = nodeFactory.apply(par, childName);
      par.addChild(lastChild);
    }
    return lastChild;
  }

  // open for package
  static void addMeasurementChild(ICacheMNode par, String mid) {
    par.addChild(getMeasurementNode(par, mid, mid + "alias"));
  }

  static IMeasurementSchema getSchema(String id) {
    return new MeasurementSchema(id, TSDataType.FLOAT);
  }

  private ICacheMNode getNode(ICacheMNode root, String path) throws MetadataException {
    String[] pathNodes = PathUtils.splitPathToDetachedNodes(path);
    ICacheMNode cur = root;
    for (String node : pathNodes) {
      if (!node.equals("root")) {
        cur = cur.getChild(node);
      }
    }
    return cur;
  }

  static ICacheMNode getInternalWithSegAddr(ICacheMNode par, String name, long segAddr) {
    ICacheMNode node = nodeFactory.createDeviceMNode(par, name).getAsMNode();
    ICachedMNodeContainer.getCachedMNodeContainer(node).setSegmentAddress(segAddr);
    return node;
  }

  static ICacheMNode getMeasurementNode(ICacheMNode par, String name, String alias) {
    IMeasurementSchema schema = new MeasurementSchema(name, TSDataType.FLOAT);
    return nodeFactory
        .createMeasurementMNode(par.getAsDeviceMNode(), name, schema, alias)
        .getAsMNode();
  }

  static void addNodeToUpdateBuffer(ICacheMNode par, ICacheMNode child) {
    ICachedMNodeContainer.getCachedMNodeContainer(par).remove(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).appendMNode(child);
    ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(child.getName());
  }

  static void moveToUpdateBuffer(ICacheMNode par, String childName) {
    ICachedMNodeContainer.getCachedMNodeContainer(par).appendMNode(par.getChild(childName));
    ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(childName);
    ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(childName);
  }

  static void moveAllToUpdate(ICacheMNode par) {
    List<String> childNames =
        par.getChildren().values().stream().map(IMNode::getName).collect(Collectors.toList());
    for (String name : childNames) {
      ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(name);
      ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(name);
    }
  }

  static void moveAllToBuffer(ICacheMNode par) {
    List<String> childNames =
        par.getChildren().values().stream().map(IMNode::getName).collect(Collectors.toList());
    for (String name : childNames) {
      ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(name);
    }
  }

  static long getSegAddrInContainer(ICacheMNode par) {
    return ICachedMNodeContainer.getCachedMNodeContainer(par).getSegmentAddress();
  }

  // endregion

  // region Tree Constructor

  static ICacheMNode virtualTriangleMTree(int size, String sgPath) throws MetadataException {
    String[] sgPathNodes = PathUtils.splitPathToDetachedNodes(sgPath);
    ICacheMNode upperNode = null;
    for (String name : sgPathNodes) {
      upperNode = nodeFactory.createInternalMNode(upperNode, name);
    }
    ICacheMNode internalNode = nodeFactory.createDatabaseDeviceMNode(upperNode, "vRoot1", 0L);

    for (int idx = 0; idx < size; idx++) {
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode<ICacheMNode> mNode =
          nodeFactory.createMeasurementMNode(
              internalNode.getAsDeviceMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode.getAsMNode());
    }

    ICacheMNode curNode = internalNode;
    for (int idx = 0; idx < size; idx++) {
      String nodeName = "int" + idx;
      ICacheMNode newNode = nodeFactory.createDeviceMNode(curNode, nodeName).getAsMNode();
      curNode.addChild(newNode);
      curNode = newNode;
    }

    for (int idx = 0; idx < 1000; idx++) {
      IMeasurementSchema schema = new MeasurementSchema("finalM" + idx, TSDataType.FLOAT);
      IMeasurementMNode<ICacheMNode> mNode =
          nodeFactory.createMeasurementMNode(
              internalNode.getAsDeviceMNode(), "finalM" + idx, schema, "finalals" + idx);
      curNode.addChild(mNode.getAsMNode());
    }
    IMeasurementSchema schema = new MeasurementSchema("finalM", TSDataType.FLOAT);
    IMeasurementMNode<ICacheMNode> mNode =
        nodeFactory.createMeasurementMNode(
            internalNode.getAsDeviceMNode(), "finalM", schema, "finalals");
    curNode.addChild(mNode.getAsMNode());
    upperNode.addChild(internalNode);
    return internalNode;
  }

  static ICacheMNode getFlatTree(int flatSize, String id) {
    ICacheMNode root = nodeFactory.createInternalMNode(null, "root");
    ICacheMNode test = nodeFactory.createInternalMNode(root, "test");
    ICacheMNode internalNode = nodeFactory.createDatabaseDeviceMNode(null, "vRoot1", 0L);

    for (int idx = 0; idx < flatSize; idx++) {
      String measurementId = id + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode<ICacheMNode> mNode =
          nodeFactory.createMeasurementMNode(
              internalNode.getAsDeviceMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode.getAsMNode());
    }

    test.addChild(internalNode);
    return internalNode;
  }

  static ICacheMNode getVerticalTree(int height, String id) {
    ICacheMNode trueRoot = nodeFactory.createInternalMNode(null, "root");
    trueRoot.addChild(nodeFactory.createInternalMNode(trueRoot, "sgvt"));
    ICacheMNode root = nodeFactory.createDatabaseDeviceMNode(null, "vt", 0L);
    int cnt = 0;
    ICacheMNode cur = root;
    while (cnt < height) {
      cur.addChild(nodeFactory.createDeviceMNode(cur, id + "_" + cnt).getAsMNode());
      cur = cur.getChild(id + "_" + cnt);
      cnt++;
    }
    trueRoot.getChild("sgvt").addChild(root);
    return root;
  }

  static Iterator<ICacheMNode> getTreeBFT(ICacheMNode root) {
    return new Iterator<ICacheMNode>() {
      final Queue<ICacheMNode> queue = new LinkedList<>();

      {
        this.queue.add(root);
      }

      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public ICacheMNode next() {
        ICacheMNode curNode = queue.poll();
        if (!curNode.isMeasurement() && curNode.getChildren().size() > 0) {
          for (ICacheMNode child : curNode.getChildren().values()) {
            queue.add(child);
          }
        }
        return curNode;
      }
    };
  }

  // endregion

}
