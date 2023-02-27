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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheMemoryManager;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.metadata.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    CacheMemoryManager.getInstance().clear();
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
        mtree =
            new MTreeBelowSGCachedImpl(
                path,
                null,
                () -> {
                  // do nothing
                },
                node -> {
                  // do nothing
                },
                0,
                new CachedSchemaRegionStatistics(0, new CachedSchemaEngineStatistics()));
      } else {
        mtree =
            new MTreeBelowSGMemoryImpl(
                path, null, new MemSchemaRegionStatistics(0, new MemSchemaEngineStatistics()));
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
  public void testSetStorageGroup() throws MetadataException {
    try {
      storageGroup = getStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));

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
      storageGroup.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap(),
          null);
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
      storageGroup.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
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
}
