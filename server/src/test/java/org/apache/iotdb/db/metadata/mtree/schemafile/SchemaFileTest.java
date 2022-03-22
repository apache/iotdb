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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
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
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.Segment;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class SchemaFileTest {

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePersistentSchema(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnablePersistentSchema(false);
  }

  @Test
  public void essentialTestSchemaFile() throws IOException, MetadataException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1");
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

    ISchemaFile nsf = SchemaFile.loadSchemaFile("root.test.vRoot1");
    Assert.assertEquals(
        "alas99999", nsf.getChildNode(int0, "mint1").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "alas", nsf.getChildNode(int1, "int1newM").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "BBBBBB", nsf.getChildNode(int4, "AAAAA").getAsMeasurementMNode().getAlias());
    Assert.assertEquals(
        "ALLLLLLLLLLLLLLLLLLLLfinalM191",
        nsf.getChildNode(int4, "finalM191").getAsMeasurementMNode().getAlias());
    printSF(nsf);
    nsf.close();
  }

  @Test
  public void inspectFile() throws MetadataException, IOException {
    essentialTestSchemaFile();
    ISchemaFile sf = SchemaFile.loadSchemaFile("root.test.vRoot1");
    System.out.println(((SchemaFile) sf).inspect());
    sf.close();
  }

  @Test
  public void testReadFromFlat() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1");

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
    ISchemaFile sf = SchemaFile.loadSchemaFile("root.test.vRoot1");

    printSF(sf);

    Iterator<IMNode> res = sf.getChildren(node);
    int cnt = 0;
    while (res.hasNext()) {
      System.out.println(res.next().getName());
      cnt++;
    }
    sf.close();
    System.out.println(cnt);
  }

  @Test
  public void readSF() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.loadSchemaFile("sgRoot");
    printSF(sf);
  }

  @Test
  public void test10KDevices() throws MetadataException, IOException {
    int i = 10000;
    IMNode sgNode = new StorageGroupMNode(null, "sgRoot", 11111111L);

    // write with empty entitiy
    while (i >= 0) {
      IMNode aDevice = new EntityMNode(sgNode, "dev_" + i);
      sgNode.addChild(aDevice);
      i--;
    }

    Iterator<IMNode> orderedTree = getTreeBFT(sgNode);
    ISchemaFile sf = SchemaFile.initSchemaFile(sgNode.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(sgNode).setSegmentAddress(0L);
    IMNode node = null;
    try {
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
    sf = SchemaFile.loadSchemaFile(sgNode.getName());
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

    // more measurement
    for (IMNode etn : sgNode.getChildren().values()) {
      int j = 100;
      while (j >= 0) {
        addMeasurementChild(etn, String.format("mtc2_%d_%d", i, j));
        j--;
      }
    }

    orderedTree = getTreeBFT(sgNode);
    sf = SchemaFile.loadSchemaFile(sgNode.getName());
    try {
      while (orderedTree.hasNext()) {
        node = orderedTree.next();
        if (!node.isMeasurement() && !node.isStorageGroup()) {
          sf.writeMNode(node);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(node.getName());
    } finally {
      sf.close();
    }

    sf = SchemaFile.loadSchemaFile("sgRoot");
    ISchemaPage page = ((SchemaFile) sf).getPageOnTest(10700);
    ((SchemaPage) page).getSegmentTest((short) 0);
    sf.close();
  }

  @Test
  public void testVerticalTree() throws MetadataException, IOException {
    ISchemaFile sf = SchemaFile.initSchemaFile("root.sgvt.vt");
    IStorageGroupMNode sgNode = new StorageGroupEntityMNode(null, "sg", 11_111L);
    sf.updateStorageGroupNode(sgNode);

    IMNode root = getVerticalTree(100, "VT");
    Iterator<IMNode> ite = getTreeBFT(root);
    while (ite.hasNext()) {
      sf.writeMNode(ite.next());
    }
    printSF(sf);

    IMNode vt1 = getNode(root, "root.VT_0.VT_1");
    IMNode vt4 = getNode(root, "root.VT_0.VT_1.VT_2.VT_3.VT_4");
    ICachedMNodeContainer.getCachedMNodeContainer(vt1).getNewChildBuffer().clear();
    addMeasurementChild(vt1, "newM");
    sf.writeMNode(vt1);
    printSF(sf);

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

    ISchemaFile nsf = SchemaFile.loadSchemaFile("root.sgvt.vt");

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
    nsf = SchemaFile.loadSchemaFile("root.sgvt.vt");

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

    printSF(nsf);
    nsf.close();
  }

  private void printSF(ISchemaFile file) throws IOException, MetadataException {
    System.out.println(((SchemaFile) file).inspect());
  }

  private SchemaPage getPage(ISchemaFile sf, long addr) throws MetadataException, IOException {
    return ((SchemaFile) sf).getPageOnTest(SchemaFile.getPageIndex(addr));
  }

  private Segment getSegment(ISchemaFile sf, long addr) throws MetadataException, IOException {
    return getPage(sf, addr).getSegmentTest(SchemaFile.getSegIndex(addr));
  }

  public static void print(Object o) {
    System.out.println(o.toString());
  }

  private IMNode virtualTriangleMTree(int size, String sgPath) throws MetadataException {
    String[] sgPathNodes = MetaUtils.splitPathToDetachedPath(sgPath);
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
              internalNode.getAsEntityMNode(), "finalM" + idx, schema, "finalals");
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

  private void addMeasurementChild(IMNode par, String mid) {
    par.addChild(getMeasurementNode(par, mid, mid + "alias"));
  }

  private IMeasurementSchema getSchema(String id) {
    return new MeasurementSchema(id, TSDataType.FLOAT);
  }

  private IMNode getNode(IMNode root, String path) throws MetadataException {
    String[] pathNodes = MetaUtils.splitPathToDetachedPath(path);
    IMNode cur = root;
    for (String node : pathNodes) {
      if (!node.equals("root")) {
        cur = cur.getChild(node);
      }
    }
    return cur;
  }

  private IMNode getFlatTree(int flatSize) {
    return getFlatTree(flatSize, "app");
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

  public static void addNodeToUpdateBuffer(IMNode par, IMNode child) {
    ICachedMNodeContainer.getCachedMNodeContainer(par).remove(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).appendMNode(child);
    ICachedMNodeContainer.getCachedMNodeContainer(par).moveMNodeToCache(child.getName());
    ICachedMNodeContainer.getCachedMNodeContainer(par).updateMNode(child.getName());
  }
}
