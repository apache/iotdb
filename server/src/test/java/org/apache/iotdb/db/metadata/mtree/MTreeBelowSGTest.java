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
package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class MTreeBelowSGTest {

  ConfigMTree root;
  IMTreeBelowSG storageGroup;

  Set<IMTreeBelowSG> usedMTree = new HashSet<>();

  protected abstract void setConfig();

  protected abstract void rollBackConfig();

  @Before
  public void setUp() throws Exception {
    setConfig();
    EnvironmentUtils.envSetUp();
    root = new ConfigMTree();
  }

  @After
  public void tearDown() throws Exception {
    root.clear();
    root = null;
    for (IMTreeBelowSG mtree : usedMTree) {
      mtree.clear();
    }
    usedMTree.clear();
    storageGroup = null;
    EnvironmentUtils.cleanEnv();
    rollBackConfig();
  }

  private IMTreeBelowSG getStorageGroup(PartialPath path) throws MetadataException {
    try {
      root.setStorageGroup(path);
      IMTreeBelowSG mtree;
      if (SchemaEngineMode.valueOf(IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode())
          .equals(SchemaEngineMode.Schema_File)) {
        mtree = new MTreeBelowSGCachedImpl(path, null, 0);
      } else {
        mtree = new MTreeBelowSGMemoryImpl(path, null, 0);
      }
      usedMTree.add(mtree);
      return mtree;
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Test
  public void testAddLeftNodePathWithAlias() throws MetadataException {
    storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");
    } catch (MetadataException e) {
      assertTrue(e instanceof AliasAlreadyExistException);
    }
  }

  @Test
  public void testAddAndQueryPath() {
    try {
      assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.a")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a")));
      storageGroup = getStorageGroup(new PartialPath("root.a"));

      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.b.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      assertNotNull(storageGroup);
      List<MeasurementPath> result =
          storageGroup.getMeasurementPaths(new PartialPath("root.a.*.s0"), false);
      result.sort(Comparator.comparing(MeasurementPath::getFullPath));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = storageGroup.getMeasurementPaths(new PartialPath("root.a.*.*.s0"), false);
      assertEquals("root.a.b.d0.s0", result.get(0).getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAddAndQueryPathWithAlias() {
    try {
      assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.a")));
      assertFalse(root.checkStorageGroupByPath(new PartialPath("root.a")));
      storageGroup = getStorageGroup(new PartialPath("root.a"));

      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      storageGroup.createTimeseries(
          new PartialPath("root.a.d0.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "status");

      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          "temperature");
      storageGroup.createTimeseries(
          new PartialPath("root.a.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

      storageGroup.createTimeseries(
          new PartialPath("root.a.b.d0.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);

    } catch (MetadataException e1) {
      e1.printStackTrace();
    }

    try {
      assertNotNull(storageGroup);

      List<MeasurementPath> result =
          storageGroup.getMeasurementPaths(new PartialPath("root.a.*.s0"), false);
      result.sort(Comparator.comparing(MeasurementPath::getFullPath));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      result = storageGroup.getMeasurementPaths(new PartialPath("root.a.*.temperature"), false);
      result.sort(Comparator.comparing(MeasurementPath::getFullPath));
      assertEquals(2, result.size());
      assertEquals("root.a.d0.s0", result.get(0).getFullPath());
      assertEquals("root.a.d1.s0", result.get(1).getFullPath());

      List<MeasurementPath> result2 =
          storageGroup.getMeasurementPathsWithAlias(
                  new PartialPath("root.a.*.s0"), 0, 0, false, false)
              .left;
      result2.sort(Comparator.comparing(MeasurementPath::getFullPath));
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.s0", result2.get(0).getFullPath());
      assertFalse(result2.get(0).isMeasurementAliasExists());
      assertEquals("root.a.d1.s0", result2.get(1).getFullPath());
      assertFalse(result2.get(1).isMeasurementAliasExists());

      result2 =
          storageGroup.getMeasurementPathsWithAlias(
                  new PartialPath("root.a.*.temperature"), 0, 0, false, false)
              .left;
      result2.sort(Comparator.comparing(MeasurementPath::getFullPath));
      assertEquals(2, result2.size());
      assertEquals("root.a.d0.temperature", result2.get(0).getFullPathWithAlias());
      assertEquals("root.a.d1.temperature", result2.get(1).getFullPathWithAlias());

      Pair<List<MeasurementPath>, Integer> result3 =
          storageGroup.getMeasurementPathsWithAlias(
              new PartialPath("root.a.**"), 2, 0, false, false);
      assertEquals(2, result3.left.size());
      assertEquals(2, result3.right.intValue());

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroup() throws MetadataException {
    try {
      storageGroup = getStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1")).getFullPath());
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s1")).getFullPath());

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been created as database", e.getMessage());
    }

    try {
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s0")).getFullPath());
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s1")).getFullPath());
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      storageGroup.deleteTimeseriesAndReturnEmptyStorageGroup(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      root.deleteStorageGroup(new PartialPath("root.laptop.d1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1.s1")));
    assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));
    assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.laptop")));
  }

  @Test
  public void testAddSubDevice() throws MetadataException {
    storageGroup = getStorageGroup(new PartialPath("root.laptop"));
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.laptop.d1.s2.b"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    assertEquals(2, storageGroup.getDevices(new PartialPath("root"), true).size());
    assertEquals(2, storageGroup.getDevices(new PartialPath("root.**"), false).size());
    assertEquals(
        2,
        storageGroup
            .getMeasurementPathsWithAlias(new PartialPath("root.**"), 0, 0, false, false)
            .left
            .size());
  }

  @Test
  public void testSearchStorageGroup() throws MetadataException {
    String path1 = "root";
    String sgPath1 = "root.vehicle";
    storageGroup = getStorageGroup(new PartialPath(sgPath1));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath(path1)));
    try {
      storageGroup.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
      storageGroup.createTimeseries(
          new PartialPath("root.vehicle.d1.s2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (MetadataException e1) {
      fail(e1.getMessage());
    }

    assertEquals(
        root.getBelongedStorageGroups(new PartialPath("root.vehicle.d1.s1")),
        Collections.singletonList(new PartialPath(sgPath1)));
  }

  @Test
  public void testCreateTimeseries() throws MetadataException {
    String sgPath = "root.sg1";
    storageGroup = getStorageGroup(new PartialPath(sgPath));

    storageGroup.createTimeseries(
        new PartialPath("root.sg1.a.b.c"),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap(),
        null);

    try {
      // mtree doesn't support nested timeseries which means MeasurementMNode is leaf of the tree.
      storageGroup.createTimeseries(
          new PartialPath("root.sg1.a.b"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
    } catch (PathAlreadyExistException e) {
      assertEquals("Path [root.sg1.a.b] already exist", e.getMessage());
    }

    IMNode node = storageGroup.getNodeByPath(new PartialPath("root.sg1.a.b"));
    assertFalse(node instanceof MeasurementMNode);
  }

  @Test
  public void testGetNodeListInLevel() throws MetadataException {

    storageGroup = getStorageGroup(new PartialPath("root.sg1"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg1.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg1.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, false).size());

    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, false).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, false).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*.s1"), 2, false).size());

    storageGroup = getStorageGroup(new PartialPath("root.sg2"));
    storageGroup.createTimeseries(
        new PartialPath("root.sg2.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);
    storageGroup.createTimeseries(
        new PartialPath("root.sg2.d2.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        null);

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.**"), 3, false).size());

    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, false).size());
    Assert.assertEquals(
        1, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, false).size());
    Assert.assertEquals(
        2, storageGroup.getNodesListInGivenLevel(new PartialPath("root.*.*.s1"), 2, false).size());
  }
}
