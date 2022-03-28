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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MTreeAboveSGTest {

  private MTreeAboveSG root = new MTreeAboveSG();

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    root = new MTreeAboveSG();
  }

  @After
  public void tearDown() throws Exception {
    root.clear();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testSetStorageGroupExceptionMessage() {
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      root.setStorageGroup(new PartialPath("root.edge1"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals(
          "some children of root.edge1 have already been set to storage group", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge2"));
      root.setStorageGroup(new PartialPath("root.edge2.access"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals("root.edge2 has already been set to storage group", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      fail("Expected exception");
    } catch (MetadataException e) {
      assertEquals("root.edge1.access has already been set to storage group", e.getMessage());
    }
  }

  @Test
  public void testAddAndPathExist() throws MetadataException {
    String path1 = "root";
    root.setStorageGroup(new PartialPath("root.laptop"));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath(path1)));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop")));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));
  }

  @Test
  public void testGetAllChildNodeNamesByPath() {
    try {
      root.setStorageGroup(new PartialPath("root.a.d0"));
      root.setStorageGroup(new PartialPath("root.a.d5"));

      // getChildNodeByPath
      Set<String> result1 = root.getChildNodeNameInNextLevel(new PartialPath("root.a.d0")).left;
      Set<String> result2 = root.getChildNodeNameInNextLevel(new PartialPath("root.a")).left;
      Set<String> result3 = root.getChildNodeNameInNextLevel(new PartialPath("root")).left;
      assertEquals(new HashSet<>(), result1);
      assertEquals(new HashSet<>(Collections.emptyList()), result2);
      assertEquals(new HashSet<>(Collections.singletonList("a")), result3);

      // if child node is nll   will return  null HashSet
      Set<String> result4 = root.getChildNodeNameInNextLevel(new PartialPath("root.a.d5")).left;
      assertEquals(result4, new HashSet<>(Collections.emptyList()));
    } catch (MetadataException e1) {
      e1.printStackTrace();
    }
  }

  @Test
  public void testSetStorageGroup() throws IllegalPathException {
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1")).getFullPath());
      assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1.s1")));
      assertTrue(root.checkStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(
          "root.laptop.d1",
          root.getBelongedStorageGroup(new PartialPath("root.laptop.d1.s1")).getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d2"));
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
    }

    try {
      root.deleteStorageGroup(new PartialPath("root.laptop.d1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d1")));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop")));
    assertTrue(root.isStorageGroupAlreadySet(new PartialPath("root.laptop.d2")));
  }

  @Test
  public void testCheckStorageGroup() {
    try {
      assertFalse(root.isStorageGroup(new PartialPath("root")));
      assertFalse(root.isStorageGroup(new PartialPath("root1.laptop.d2")));

      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      root.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(root.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(root.isStorageGroup(new PartialPath("root.laptop.d3")));

      root.setStorageGroup(new PartialPath("root.1"));
      assertTrue(root.isStorageGroup(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      root.setStorageGroup(new PartialPath("root.laptop.d2"));

      List<PartialPath> list = new ArrayList<>();

      list.add(new PartialPath("root.laptop.d1"));
      assertEquals(list, root.getBelongedStorageGroups(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, root.getBelongedStorageGroups(new PartialPath("root.laptop.d1")));

      list.add(new PartialPath("root.laptop.d2"));
      assertEquals(list, root.getBelongedStorageGroups(new PartialPath("root.laptop.**")));
      assertEquals(list, root.getBelongedStorageGroups(new PartialPath("root.**")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    try {
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          root.getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(root.getBelongedStorageGroups(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(root.getBelongedStorageGroups(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          root.getBelongedStorageGroups(new PartialPath("root.vehicle.device.sensor")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(root.getBelongedStorageGroups(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(root.getBelongedStorageGroups(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testIllegalStorageGroup() {
    try {
      root.setStorageGroup(new PartialPath("root.\"sg.ln\""));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "The storage group name can only be characters, numbers and underscores. root.\"sg.ln\" is not a legal path",
          e.getMessage());
    }
  }

  @Test
  public void testCountStorageGroup() throws MetadataException {
    root.setStorageGroup(new PartialPath("root.sg1"));
    root.setStorageGroup(new PartialPath("root.a.sg1"));
    root.setStorageGroup(new PartialPath("root.a.b.sg1"));
    root.setStorageGroup(new PartialPath("root.sg2"));
    root.setStorageGroup(new PartialPath("root.a.sg2"));
    root.setStorageGroup(new PartialPath("root.sg3"));
    root.setStorageGroup(new PartialPath("root.a.b.sg3"));

    Assert.assertEquals(7, root.getStorageGroupNum(new PartialPath("root.**"), false));
    Assert.assertEquals(3, root.getStorageGroupNum(new PartialPath("root.*"), false));
    Assert.assertEquals(2, root.getStorageGroupNum(new PartialPath("root.*.*"), false));
    Assert.assertEquals(2, root.getStorageGroupNum(new PartialPath("root.*.*.*"), false));
    Assert.assertEquals(1, root.getStorageGroupNum(new PartialPath("root.*.sg1"), false));
    Assert.assertEquals(2, root.getStorageGroupNum(new PartialPath("root.**.sg1"), false));
    Assert.assertEquals(1, root.getStorageGroupNum(new PartialPath("root.sg3"), false));
    Assert.assertEquals(2, root.getStorageGroupNum(new PartialPath("root.*.b.*"), false));
  }

  @Test
  public void testGetNodeListInLevel() throws MetadataException {
    root.setStorageGroup(new PartialPath("root.sg1"));

    root.setStorageGroup(new PartialPath("root.sg2"));
    SchemaEngine.StorageGroupFilter filter = storageGroup -> storageGroup.equals("root.sg1");

    Pair<List<PartialPath>, Set<PartialPath>> result =
        root.getNodesListInGivenLevel(new PartialPath("root.**"), 3, null);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, null);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, null);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.**"), 3, filter);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(1, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.*.**"), 2, filter);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(1, result.right.size());
  }
}
