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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.confignode.persistence.schema.mnode.IConfigMNode;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigMTreeTest {

  private ConfigMTree root;

  @Before
  public void setUp() throws Exception {
    root = new ConfigMTree();
  }

  @After
  public void tearDown() throws Exception {
    root.clear();
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testSetStorageGroupExceptionMessage() {
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      root.setStorageGroup(new PartialPath("root.edge1"));
      fail("Expected exception");
    } catch (final MetadataException e) {
      assertEquals(
          "some children of root.edge1 have already been created as database", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge2"));
      root.setStorageGroup(new PartialPath("root.edge2.access"));
      fail("Expected exception");
    } catch (final MetadataException e) {
      assertEquals("root.edge2 has already been created as database", e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.edge1.access"));
      fail("Expected exception");
    } catch (final MetadataException e) {
      assertEquals("root.edge1.access has already been created as database", e.getMessage());
    }
  }

  @Test
  public void testAddAndPathExist() throws MetadataException {
    final String path1 = "root";
    root.setStorageGroup(new PartialPath("root.laptop"));
    assertTrue(root.isDatabaseAlreadySet(new PartialPath(path1)));
    assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop")));
    assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop.d1")));
  }

  @Test
  public void testSetStorageGroup() throws IllegalPathException {
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop.d1")));
      assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop.d1.s1")));
    } catch (final MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d2"));
    } catch (final MetadataException e) {
      fail(e.getMessage());
    }
    try {
      root.setStorageGroup(new PartialPath("root.laptop"));
    } catch (final MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been created as database", e.getMessage());
    }

    try {
      root.deleteDatabase(new PartialPath("root.laptop.d1"));
    } catch (final MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(root.isDatabaseAlreadySet(new PartialPath("root.laptop.d1")));
    assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop")));
    assertTrue(root.isDatabaseAlreadySet(new PartialPath("root.laptop.d2")));
  }

  @Test
  public void testGetAllFileNamesByPath() {
    try {
      root.setStorageGroup(new PartialPath("root.laptop.d1"));
      root.setStorageGroup(new PartialPath("root.laptop.d2"));

      final List<PartialPath> list = new ArrayList<>();

      list.add(new PartialPath("root.laptop.d1"));
      assertEquals(list, root.getBelongedDatabases(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, root.getBelongedDatabases(new PartialPath("root.laptop.d1")));

      list.add(new PartialPath("root.laptop.d2"));
      assertEquals(list, root.getBelongedDatabases(new PartialPath("root.laptop.**")));
      assertEquals(list, root.getBelongedDatabases(new PartialPath("root.**")));
    } catch (final MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    try {
      assertTrue(root.getBelongedDatabases(new PartialPath("root")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle.device0")).isEmpty());
      assertTrue(
          root.getBelongedDatabases(new PartialPath("root.vehicle.device0.sensor")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(root.getBelongedDatabases(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(root.getBelongedDatabases(new PartialPath("root.vehicle.device0")).isEmpty());
      assertFalse(
          root.getBelongedDatabases(new PartialPath("root.vehicle.device0.sensor")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle1.device0")).isEmpty());

      root.setStorageGroup(new PartialPath("root.vehicle1.device0"));
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(root.getBelongedDatabases(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(root.getBelongedDatabases(new PartialPath("root.vehicle1.device0")).isEmpty());
    } catch (final MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testIllegalStorageGroup() {
    try {
      root.setStorageGroup(new PartialPath("root.\"sg.ln\""));
    } catch (final MetadataException e) {
      Assert.assertEquals("root.\"sg.ln\" is not a legal path", e.getMessage());
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

    assertEquals(7, root.getDatabaseNum(new PartialPath("root.**"), ALL_MATCH_SCOPE, false));
    assertEquals(3, root.getDatabaseNum(new PartialPath("root.*"), ALL_MATCH_SCOPE, false));
    assertEquals(2, root.getDatabaseNum(new PartialPath("root.*.*"), ALL_MATCH_SCOPE, false));
    assertEquals(2, root.getDatabaseNum(new PartialPath("root.*.*.*"), ALL_MATCH_SCOPE, false));
    assertEquals(1, root.getDatabaseNum(new PartialPath("root.*.sg1"), ALL_MATCH_SCOPE, false));
    assertEquals(2, root.getDatabaseNum(new PartialPath("root.**.sg1"), ALL_MATCH_SCOPE, false));
    assertEquals(1, root.getDatabaseNum(new PartialPath("root.sg3"), ALL_MATCH_SCOPE, false));
    assertEquals(2, root.getDatabaseNum(new PartialPath("root.*.b.*"), ALL_MATCH_SCOPE, false));
  }

  @Test
  public void testGetNodeListInLevel() throws MetadataException {
    root.setStorageGroup(new PartialPath("root.sg1"));

    root.setStorageGroup(new PartialPath("root.sg2"));

    Pair<List<PartialPath>, Set<PartialPath>> result =
        root.getNodesListInGivenLevel(new PartialPath("root.**"), 3, false, ALL_MATCH_SCOPE);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.**"), 1, false, ALL_MATCH_SCOPE);
    Assert.assertEquals(2, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.*.*"), 2, false, ALL_MATCH_SCOPE);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    result = root.getNodesListInGivenLevel(new PartialPath("root.*.*"), 1, false, ALL_MATCH_SCOPE);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(2, result.right.size());

    root.setStorageGroup(new PartialPath("root.test.`001.002.003`"));
    root.setStorageGroup(new PartialPath("root.test.g_0.s_0_b001"));
    root.setStorageGroup(new PartialPath("root.sg"));
    root.setStorageGroup(new PartialPath("root.ln"));

    result =
        root.getNodesListInGivenLevel(new PartialPath("root.*.*.s1"), 2, true, ALL_MATCH_SCOPE);
    Assert.assertEquals(0, result.left.size());
    Assert.assertEquals(5, result.right.size());
    Assert.assertTrue(result.right.contains(new PartialPath("root.sg1")));
    Assert.assertTrue(result.right.contains(new PartialPath("root.sg2")));
    Assert.assertTrue(result.right.contains(new PartialPath("root.sg")));
    Assert.assertTrue(result.right.contains(new PartialPath("root.ln")));
    Assert.assertTrue(result.right.contains(new PartialPath("root.test.`001.002.003`")));
  }

  @Test
  public void testSerialization() throws Exception {
    final PartialPath[] pathList =
        new PartialPath[] {
          new PartialPath("root.sg"),
          new PartialPath("root.a.sg"),
          new PartialPath("root.a.b.sg"),
          new PartialPath("root.a.a.b.sg")
        };
    for (int i = 0; i < pathList.length; i++) {
      root.setStorageGroup(pathList[i]);
      final IDatabaseMNode<IConfigMNode> storageGroupMNode =
          root.getDatabaseNodeByDatabasePath(pathList[i]);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setDataReplicationFactor(i);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setSchemaReplicationFactor(i);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setTimePartitionInterval(i);
      root.getNodeWithAutoCreate(pathList[i].concatNode("a")).setSchemaTemplateId(i);
    }

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    root.serialize(outputStream);

    final ConfigMTree newTree = new ConfigMTree();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    newTree.deserialize(inputStream);

    for (int i = 0; i < pathList.length; i++) {
      final TDatabaseSchema storageGroupSchema =
          newTree.getDatabaseNodeByDatabasePath(pathList[i]).getAsMNode().getDatabaseSchema();
      Assert.assertEquals(i, storageGroupSchema.getSchemaReplicationFactor());
      Assert.assertEquals(i, storageGroupSchema.getDataReplicationFactor());
      Assert.assertEquals(i, storageGroupSchema.getTimePartitionInterval());
      assertEquals(
          i, newTree.getNodeWithAutoCreate(pathList[i].concatNode("a")).getSchemaTemplateId());
    }

    assertEquals(
        3,
        newTree.getMatchedDatabases(new PartialPath("root.**.sg"), ALL_MATCH_SCOPE, false).size());
    assertEquals(
        2,
        newTree
            .getMatchedDatabases(new PartialPath("root.**.b.sg"), ALL_MATCH_SCOPE, false)
            .size());
    assertEquals(
        1,
        newTree.getMatchedDatabases(new PartialPath("root.*.*.sg"), ALL_MATCH_SCOPE, false).size());
    assertEquals(
        3, newTree.getMatchedDatabases(new PartialPath("root.a"), ALL_MATCH_SCOPE, true).size());
    assertEquals(
        1, newTree.getMatchedDatabases(new PartialPath("root.a.b"), ALL_MATCH_SCOPE, true).size());
    assertEquals(
        1,
        newTree.getMatchedDatabases(new PartialPath("root.a.b.sg"), ALL_MATCH_SCOPE, true).size());
  }

  @Test
  public void testTableSerialization() throws Exception {
    final PartialPath[] pathList =
        new PartialPath[] {
          new PartialPath("root.sg"),
          new PartialPath("root.a.sg"),
          new PartialPath("root.a.b.sg"),
          new PartialPath("root.a.a.b.sg")
        };

    for (int i = 0; i < pathList.length; i++) {
      root.setStorageGroup(pathList[i]);
      final IDatabaseMNode<IConfigMNode> storageGroupMNode =
          root.getDatabaseNodeByDatabasePath(pathList[i]);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setDataReplicationFactor(i);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setSchemaReplicationFactor(i);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setTimePartitionInterval(i);
      storageGroupMNode.getAsMNode().getDatabaseSchema().setIsTableModel(true);

      final String tableName = "table" + i;
      final TsTable table = new TsTable(tableName);
      table.addColumnSchema(new IdColumnSchema("Id", TSDataType.STRING));
      table.addColumnSchema(new AttributeColumnSchema("Attr", TSDataType.STRING));
      table.addColumnSchema(
          new MeasurementColumnSchema(
              "Measurement", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));

      root.preCreateTable(pathList[i], table);
      root.commitCreateTable(pathList[i], tableName);
    }

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    root.serialize(outputStream);

    final ConfigMTree newTree = new ConfigMTree();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    newTree.deserialize(inputStream);

    for (int i = 0; i < pathList.length; i++) {
      final TDatabaseSchema storageGroupSchema =
          newTree.getDatabaseNodeByDatabasePath(pathList[i]).getAsMNode().getDatabaseSchema();
      Assert.assertEquals(i, storageGroupSchema.getSchemaReplicationFactor());
      Assert.assertEquals(i, storageGroupSchema.getDataReplicationFactor());
      Assert.assertEquals(i, storageGroupSchema.getTimePartitionInterval());
    }

    assertEquals(
        3,
        newTree.getMatchedDatabases(new PartialPath("root.**.sg"), ALL_MATCH_SCOPE, false).size());
    assertEquals(
        2,
        newTree
            .getMatchedDatabases(new PartialPath("root.**.b.sg"), ALL_MATCH_SCOPE, false)
            .size());
    assertEquals(
        1,
        newTree.getMatchedDatabases(new PartialPath("root.*.*.sg"), ALL_MATCH_SCOPE, false).size());
    assertEquals(
        3, newTree.getMatchedDatabases(new PartialPath("root.a"), ALL_MATCH_SCOPE, true).size());
    assertEquals(
        1, newTree.getMatchedDatabases(new PartialPath("root.a.b"), ALL_MATCH_SCOPE, true).size());
    assertEquals(
        1,
        newTree.getMatchedDatabases(new PartialPath("root.a.b.sg"), ALL_MATCH_SCOPE, true).size());

    for (int i = 0; i < pathList.length; i++) {
      final List<TsTable> tables = newTree.getAllUsingTablesUnderSpecificDatabase(pathList[i]);
      assertEquals(1, tables.size());
      final TsTable table = tables.get(0);
      assertEquals("table" + i, table.getTableName());
      assertEquals(1, table.getIdNums());
      assertEquals(4, table.getColumnNum());
    }
  }

  @Test
  public void testSetTemplate() throws MetadataException {
    root.setStorageGroup(new PartialPath("root.a"));
    PartialPath path = new PartialPath("root.a.template0");
    try {
      root.checkTemplateOnPath(path);
    } catch (MetadataException e) {
      fail();
    }

    IConfigMNode node = root.getNodeWithAutoCreate(path);
    node.setSchemaTemplateId(0);

    try {
      root.checkTemplateOnPath(path);
      fail();
    } catch (MetadataException ignore) {
    }

    path = new PartialPath("root.a.b.template0");
    node = root.getNodeWithAutoCreate(path);
    node.setSchemaTemplateId(0);

    try {
      root.checkTemplateOnPath(path);
      fail();
    } catch (MetadataException ignore) {
    }

    try {
      List<String> pathList = root.getPathsSetOnTemplate(0, ALL_MATCH_SCOPE, false);
      Assert.assertTrue(pathList.contains("root.a.template0"));
      Assert.assertTrue(pathList.contains("root.a.b.template0"));
    } catch (MetadataException e) {
      fail();
    }
  }
}
