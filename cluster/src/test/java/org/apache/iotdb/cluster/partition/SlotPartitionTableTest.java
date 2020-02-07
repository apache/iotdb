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
package org.apache.iotdb.cluster.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.nodetool.function.Partition;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator.PropertyType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotPartitionTableTest {
  Logger logger = LoggerFactory.getLogger(SlotPartitionTableTest.class);
  SlotPartitionTable localTable;
  Node localNode;
  int replica_size = 5;
  MManager[] mManager;

  SlotPartitionTable[] tables;//The PartitionTable on each node.
  List<Node> nodes;

  @Before
  public void setUp() throws MetadataException {
    MManager.getInstance().init();
    nodes = new ArrayList<>();
    IntStream.range(0, 20).forEach(i -> nodes.add(new Node("localhost", 30000 + i, i, 40000 + i)));
    localNode = nodes.get(3);
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(replica_size);
    tables = new SlotPartitionTable[20];
    mManager = new MManager[20];

    //suppose there are 40 storage groups and each node maintains two of them.
    String[] storageNames = new String[40];
    List<String>[] nodeSGs = new ArrayList[20];
    for (int i = 0; i < 20; i ++) {
      nodeSGs[i] = new ArrayList<>();
      tables[i] = new SlotPartitionTable(nodes, nodes.get(i));
    }
    localTable = tables[3];
    //thisNode hold No. 1500 to 1999 slot.

    for (int i = 0; i < 20; i ++) {
      storageNames[i] = String.format("root.sg.l2.l3.%d", i);
      //determine which node the sg belongs to
      Node node = localTable.routeToHeader(storageNames[i], 0);
      nodeSGs[node.getMetaPort() - 30000].add(storageNames[i]);
      storageNames[i + 20] = String.format("root.sg.l2.l3.l4.%d", i + 20);
      node = localTable.routeToHeader(storageNames[i + 20], 0);
      nodeSGs[node.getMetaPort() - 30000].add(storageNames[i + 20]);
    }
    for (int i = 0; i < 20; i ++) {
      mManager[i] = MManagerWhiteBox.newMManager("target/schemas/mlog_" + i);
      initMockMManager(i, mManager[i], storageNames, nodeSGs[i]);
      Whitebox.setInternalState(tables[i], "mManager", mManager[i]);
    }
  }

  private void initMockMManager(int id, MManager mmanager, String[] storageGroups, List<String> ownedSGs)
      throws MetadataException {
    for (String sg : storageGroups) {
      mmanager.setStorageGroupToMTree(sg);
    }
    for (String sg : ownedSGs) {
      //register 4 series;
      for (int i = 0; i < 4; i ++) {
        try {
          mmanager.addPathToMTree(String.format(sg + ".ld.l1.d%d.s%d", i/2, i%2), "INT32", "RLE");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  @After
  public void tearDown() {
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(3);
    MManager.getInstance().clear();
    if (mManager != null) {
      for (MManager manager : mManager) {
        manager.clear();
      }
    }
    for (File file : new File("target/schemas").listFiles()) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.error("{} can not be deleted.", file, e);
      }
    }
  }

  @Test
  public void testGetHeaderGroup() {
    Arrays.stream(new int[]{10, 15, 19}).forEach( i -> {
      int last = (i + replica_size - 1) % 20;
      assertGetHeaderGroup(i, last);
    });
  }

  private void assertGetHeaderGroup(int start, int last) {
    PartitionGroup group = localTable
        .getHeaderGroup(new Node("localhost", 30000 + start, start, 40000 + start));
    assertEquals(replica_size, group.size());
    assertEquals(new Node("localhost", 30000 + start, start, 40000 + start), group.getHeader());
    assertEquals(
        new Node("localhost", 30000 + last,  last, 40000 +  last),
        group.get(replica_size - 1));
  }
  private void assertPartitionGroup(PartitionGroup group, int... nodeIds) {
    for (int i = 0; i < nodeIds.length; i++) {
      assertEquals(nodeIds[i], group.get(i).nodeIdentifier);
    }
  }

  @Test
  public void testRoute() {
    PartitionGroup group1 = localTable.route("root.sg1", 1);
    PartitionGroup group2 = localTable.route("root.sg1", 2);
    PartitionGroup group3 = localTable.route("root.sg2", 2);
    PartitionGroup group4 = localTable.route("root.sg1", 2 + StorageEngine.getTimePartitionInterval());

    assertEquals(group1, group2);
    assertNotEquals(group2, group3);
    assertNotEquals(group3, group4);

    PartitionGroup group = localTable.route(ClusterConstant.SLOT_NUM + 1);
    assertNull(group);
    //thisNode hold No. 1500 to 1999 slot.
    group1 = localTable.route(1501);
    group2 = localTable.route(1502);
    group3 = localTable.route(2501);
    assertEquals(group1, group2);
    assertNotEquals(group2, group3);
  }

  @Test
  public void getPartitionKey() {
    //only accept a storage name
    int slot1 = localTable.getPartitionKey("root.sg.l2.l3.l4.28", 0);
    int slot2 = localTable.getPartitionKey("root.sg.l2.l3.l4.28", 1);
    int slot3 = localTable.getPartitionKey("root.sg.l2.l3.l4.29", 0);
    int slot4 = localTable.getPartitionKey("root.sg.l2.l3.l4.29", StorageEngine.getTimePartitionInterval());
    assertEquals(slot1, slot2);
    assertNotEquals(slot1, slot3);
    assertNotEquals(slot3, slot4);
  }

  @Test
  public void routeToHeader() {
    Node node1 = localTable.routeToHeader("root.sg.l2.l3.l4.28", 0);
    Node node2 = localTable.routeToHeader("root.sg.l2.l3.l4.28", 1);
    Node node3 = localTable.routeToHeader("root.sg.l2.l3.l4.28", 1 + StorageEngine.getTimePartitionInterval());
    assertEquals(node1, node2);
    assertNotEquals(node2, node3);
  }

  @Test
  public void addNode() {
    //TODO do it when delete node is finished.
  }

  @Test
  public void getLocalGroups() {
    List<PartitionGroup> groups = localTable.getLocalGroups();
    int[][] nodeIds = new int[replica_size][replica_size];
    //we write them clearly to help people understand how the replica is assigned.
    nodeIds[0] = new int[]{3, 4, 5, 6, 7};
    nodeIds[1] = new int[]{2, 3, 4, 5, 6};
    nodeIds[2] = new int[]{1, 2, 3, 4, 5};
    nodeIds[3] = new int[]{0, 1, 2, 3, 4};
    nodeIds[4] = new int[]{19, 0, 1, 2, 3};
    for (int i = 0; i < nodeIds.length; i++) {
      assertPartitionGroup(groups.get(i), nodeIds[i]);
    }

  }

  @Test
  public void serializeAndDeserialize() {
    ByteBuffer buffer = localTable.serialize();
    SlotPartitionTable tmpTable = new SlotPartitionTable(new Node());
    tmpTable.deserialize(buffer);
    assertTrue(localTable.equals(tmpTable));
  }

  @Test
  public void getAllNodes() {
    assertEquals(20, localTable.getAllNodes().size());
  }

  @Test
  public void getPreviousNodeMap() {
    //before adding or deleting node, it should be null
    assertNull(localTable.getPreviousNodeMap(localNode));
    //TODO after adding or deleting node, it has data
  }

  @Test
  public void getNodeSlots() {
    //TODO only meaningful when nodelist changes
  }

  @Test
  public void getAllNodeSlots() {
    //TODO only meaningful when nodelist changes
  }

  @Test
  public void getTotalSlotNumbers() {
    assertEquals(ClusterConstant.SLOT_NUM, localTable.getTotalSlotNumbers());
  }

  @Test
  public void getMManager() {
    //no meanningful
  }

  public void testPhysicalPlan() {
    PhysicalPlan aggregationPlan = new AggregationPlan();
    assertTrue(PartitionUtils.isLocalPlan(aggregationPlan));
    PhysicalPlan deletePlan = new DeletePlan();
    assertTrue(PartitionUtils.isGlobalPlan(deletePlan));
    PhysicalPlan fillQueryPlan = new FillQueryPlan();
    assertTrue(PartitionUtils.isLocalPlan(fillQueryPlan));
    PhysicalPlan groupByPlan = new GroupByPlan();
    assertTrue(PartitionUtils.isLocalPlan(groupByPlan));
    PhysicalPlan queryPlan = new QueryPlan();
    assertTrue(PartitionUtils.isLocalPlan(queryPlan));
    PhysicalPlan updatePlan = new UpdatePlan();
    try {
     localTable.routePlan(updatePlan);
    } catch (UnsupportedPlanException e) {
      //success
    } catch (StorageGroupNotSetException | IllegalPathException e) {
      fail(e.getMessage());
    }
    try {
      PhysicalPlan authorPlan = new AuthorPlan(AuthorType.CREATE_ROLE, "test", "test", "test", "test", new String[]{},  new Path("root.sg.l2.l3.l4.28.ld.l1.d0"));
      assertTrue(PartitionUtils.isGlobalPlan(authorPlan));
    } catch (AuthException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    PhysicalPlan dataAuthPlan = new DataAuthPlan(OperatorType.AUTHOR, Collections.emptyList());
    assertTrue(PartitionUtils.isGlobalPlan(dataAuthPlan));
    PhysicalPlan deleteStorageGroup = new DeleteStorageGroupPlan(Collections.emptyList());
    assertTrue(PartitionUtils.isGlobalPlan(deleteStorageGroup));
    PhysicalPlan loadConfigPlan = new LoadConfigurationPlan();
    assertTrue(PartitionUtils.isGlobalPlan(loadConfigPlan));
    PhysicalPlan operateFilePlan = new OperateFilePlan(new File(""), OperatorType.TABLESCAN);
    assertTrue(PartitionUtils.isLocalPlan(operateFilePlan));
    PhysicalPlan propertyPlan = new PropertyPlan(PropertyType.ADD_TREE, new Path(""), new Path(""));
    try {
      localTable.routePlan(propertyPlan);
    } catch (UnsupportedPlanException e) {
      //success
    } catch (StorageGroupNotSetException | IllegalPathException e) {
      fail(e.getMessage());
    }
    PhysicalPlan setStorageGroupPlan = new SetStorageGroupPlan();
    assertTrue(PartitionUtils.isGlobalPlan(setStorageGroupPlan));
    PhysicalPlan setTTLPlan = new SetTTLPlan("");
    assertTrue(PartitionUtils.isGlobalPlan(setTTLPlan));
    PhysicalPlan showPlan = new ShowPlan(ShowContentType.DYNAMIC_PARAMETER);
    assertTrue(PartitionUtils.isLocalPlan(showPlan));
    showPlan = new ShowPlan(ShowContentType.FLUSH_TASK_INFO);
    assertTrue(PartitionUtils.isLocalPlan(showPlan));
    showPlan = new ShowPlan(ShowContentType.VERSION);
    assertTrue(PartitionUtils.isLocalPlan(showPlan));
    showPlan = new ShowPlan(ShowContentType.TTL);
    assertTrue(PartitionUtils.isLocalPlan(showPlan));

  }

  @Test
  public void testInsertPlan() {
    PhysicalPlan insertPlan1 = new InsertPlan("root.sg.l2.l3.l4.28.ld.l1.d0", 1, new String[]{"s0", "s1"}, new String[]{"0", "1"});
    PhysicalPlan insertPlan2 = new InsertPlan("root.sg.l2.l3.l4.28.ld.l1.d0", 1 + StorageEngine.getTimePartitionInterval(), new String[]{"s0", "s1"}, new String[]{"0", "1"});
    PartitionGroup group1, group2;
    assertFalse(insertPlan1.canbeSplit());
    try {
      group1 = localTable.routePlan(insertPlan1);
      group2 = localTable.routePlan(insertPlan2);
      assertNotEquals(group1, group2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeSeriesPlan() {
    PhysicalPlan createTimeSeriesPlan1 = new CreateTimeSeriesPlan(new Path("root.sg.l2.l3.l4.28.ld.l1.d1"), TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY, Collections
        .emptyMap());
    PhysicalPlan createTimeSeriesPlan2 = new CreateTimeSeriesPlan(new Path("root.sg.l2.l3.l4.28.ld.l1.d2"), TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY, Collections
        .emptyMap());
    PhysicalPlan createTimeSeriesPlan3 = new CreateTimeSeriesPlan(new Path("root.sg.l2.l3.l4.29.ld.l1.d2"), TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY, Collections
        .emptyMap());
    assertFalse(createTimeSeriesPlan1.canbeSplit());
    try {
      PartitionGroup group1 = localTable.routePlan(createTimeSeriesPlan1);
      PartitionGroup group2 = localTable.routePlan(createTimeSeriesPlan2);
      PartitionGroup group3 = localTable.routePlan(createTimeSeriesPlan3);
      assertEquals(group1, group2);
      assertNotEquals(group2, group3);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testBatchInsertPlan() {
    PhysicalPlan batchInertPlan = new BatchInsertPlan("root.sg.l2.l3.l4.28.ld.l1.d0", new String[]{"s0", "s1"}, Arrays.asList(0, 1));
    assertTrue(batchInertPlan.canbeSplit());
    //(String deviceId, String[] measurements, List<Integer> dataTypes)
    long[] times = new long[9];
    Object[] values = new Object[2];
    values[0] = new boolean[9];
    values[1] = new int[9];

    for (int i = 0; i < 3; i++) {
      times[i] = Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[])values[0])[i] = new Random().nextBoolean();
      ((int[])values[1])[i] = new Random().nextInt();
    }
    for (int i = 3; i < 6; i++) {
      times[i] = StorageEngine.getTimePartitionInterval() +
          Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[])values[0])[i] = new Random().nextBoolean();
      ((int[])values[1])[i] = new Random().nextInt();
    }
    for (int i = 6; i < 9; i++) {
      times[i] = StorageEngine.getTimePartitionInterval() * 10
          + Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[])values[0])[i] = new Random().nextBoolean();
      ((int[])values[1])[i] = new Random().nextInt();
    }
    ((BatchInsertPlan)batchInertPlan).setTimes(times);
    ((BatchInsertPlan)batchInertPlan).setColumns(values);
    ((BatchInsertPlan)batchInertPlan).setRowCount(9);
    try {
      Map<PhysicalPlan, PartitionGroup> result = localTable.splitAndRoutePlan(batchInertPlan);
      assertEquals(3, result.size());
      result.forEach( (key, value) -> {
        assertEquals(3, ((BatchInsertPlan) key).getRowCount());
        long[] subtimes = ((BatchInsertPlan) key).getTimes();
        assertEquals(3, subtimes.length);
        assertEquals(subtimes[0]/StorageEngine.getTimePartitionInterval(),
            subtimes[2]/StorageEngine.getTimePartitionInterval());
      });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCountPlan() {
    PhysicalPlan countPlan1 = new CountPlan(ShowContentType.COUNT_TIMESERIES,new Path("root.sg.*.l3.l4.28.*"));
    PhysicalPlan countPlan2 = new CountPlan(ShowContentType.COUNT_TIMESERIES,new Path("root.sg.*.l3.*"));
    PhysicalPlan countPlan3 = new CountPlan(ShowContentType.COUNT_NODE_TIMESERIES,new Path("root.sg.l2.l3.l4.28"));
    PhysicalPlan countPlan4 = new CountPlan(ShowContentType.COUNT_NODE_TIMESERIES,new Path("root.sg.l2.l3"), 6);
    PhysicalPlan countPlan5 = new CountPlan(ShowContentType.COUNT_NODES,new Path("root.sg.l2.l3"), 5);
    try {
      assertTrue(countPlan1.canbeSplit());
      Map<PhysicalPlan, PartitionGroup> result1 = localTable.splitAndRoutePlan(countPlan1);
      assertEquals(1, result1.size());
      Map<PhysicalPlan, PartitionGroup> result2 = localTable.splitAndRoutePlan(countPlan2);
      assertEquals(40, result2.size());
      Map<PhysicalPlan, PartitionGroup> result3 = localTable.splitAndRoutePlan(countPlan3);
      assertEquals(1, result3.size());
      Map<PhysicalPlan, PartitionGroup> result4 = localTable.splitAndRoutePlan(countPlan4);
      //TODO this case can be optimized
      assertEquals(40, result4.size());
      Map<PhysicalPlan, PartitionGroup> result5 = localTable.splitAndRoutePlan(countPlan5);
      //TODO this case can be optimized
      assertEquals(40, result5.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowChildPathsPlan() {
    PhysicalPlan showChildPathsPlan1 = new ShowChildPathsPlan(ShowContentType.CHILD_PATH, new Path("root.sg.l2.l3.l4.28"));
    PhysicalPlan showChildPathsPlan2 = new ShowChildPathsPlan(ShowContentType.CHILD_PATH, new Path("root.sg.l2.l3.l4"));
    try {
      assertFalse(showChildPathsPlan1.canbeSplit());
      PartitionGroup group1= localTable.routePlan(showChildPathsPlan1);
      PartitionGroup group2= localTable.routePlan(showChildPathsPlan2);
      assertNotEquals(group1, group2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowDevicesPlan() {
    //TODO this case can be optimized
    PhysicalPlan showDevicesPlan1 = new ShowDevicesPlan(ShowContentType.DEVICES, new Path("root.*.l2"));
    PhysicalPlan showDevicesPlan2 = new ShowDevicesPlan(ShowContentType.DEVICES, new Path("root.sg.l2.l3.l4"));
    assertTrue(showDevicesPlan1.canbeSplit());

    try {
      Map<PhysicalPlan, PartitionGroup> group1 = localTable.splitAndRoutePlan(showDevicesPlan1);
      Map<PhysicalPlan, PartitionGroup> group2 = localTable.splitAndRoutePlan(showDevicesPlan2);
      assertEquals(40, group1.size());
      assertEquals(20, group2.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
  @Test
  public void testShowTimeSeriesPlan() {
    //TODO this case can be optimized
    PhysicalPlan showDevicesPlan1 = new ShowTimeSeriesPlan(ShowContentType.TIMESERIES, new Path("root.*.l2"));
    PhysicalPlan showDevicesPlan2 = new ShowDevicesPlan(ShowContentType.TIMESERIES, new Path("root.sg.l2.l3.l4"));
    assertTrue(showDevicesPlan1.canbeSplit());

    try {
      Map<PhysicalPlan, PartitionGroup> group1 = localTable.splitAndRoutePlan(showDevicesPlan1);
      Map<PhysicalPlan, PartitionGroup> group2 = localTable.splitAndRoutePlan(showDevicesPlan2);
      assertEquals(40, group1.size());
      assertEquals(20, group2.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
