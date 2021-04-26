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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SerializeUtilTest {

  @Test
  public void testSlotPartitionTable() {
    List<Node> nodes = new ArrayList<>();
    nodes.add(TestUtils.getNode(0));
    nodes.add(TestUtils.getNode(1));
    nodes.add(TestUtils.getNode(2));
    SlotPartitionTable slotPartitionTable1 = new SlotPartitionTable(nodes, TestUtils.getNode(0));
    SlotPartitionTable slotPartitionTable2 = new SlotPartitionTable(nodes, TestUtils.getNode(0));
    SlotPartitionTable slotPartitionTable3 = new SlotPartitionTable(nodes, TestUtils.getNode(0));
    slotPartitionTable1.removeNode(TestUtils.getNode(0));
    slotPartitionTable2.deserialize(slotPartitionTable1.serialize());
    assertEquals(slotPartitionTable2, slotPartitionTable1);
    slotPartitionTable1.addNode(TestUtils.getNode(0));
    slotPartitionTable3.deserialize(slotPartitionTable1.serialize());
    assertEquals(slotPartitionTable3, slotPartitionTable1);
  }

  @Test
  public void testStrToNode() {
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      String nodeStr = node.toString();
      Node fromStr = ClusterUtils.stringToNode(nodeStr);
      assertEquals(node, fromStr);
    }
  }

  @Test
  public void testInsertTabletPlanLog()
      throws UnknownLogTypeException, IOException, IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((float[]) columns[1])[r] = 2;
      ((long[]) columns[2])[r] = 10000;
      ((int[]) columns[3])[r] = 100;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.test"),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setStart(0);
    tabletPlan.setEnd(4);

    Log log = new PhysicalPlanLog(tabletPlan);
    log.setCurrLogTerm(1);
    log.setCurrLogIndex(2);

    ByteBuffer serialized = log.serialize();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    tabletPlan.serialize(dataOutputStream);
    ByteBuffer bufferA = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    ByteBuffer bufferB = ByteBuffer.allocate(bufferA.limit());
    tabletPlan.serialize(bufferB);
    bufferB.flip();
    assertEquals(bufferA, bufferB);

    Log parsed = LogParser.getINSTANCE().parse(serialized);
    assertEquals(log, parsed);
  }

  @Test
  public void testCreateMultiTimeSeriesPlanLog()
      throws UnknownLogTypeException, IOException, IllegalPathException {
    List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath("root.sg1.d2.s1"));
    paths.add(new PartialPath("root.sg1.d2.s2"));
    List<TSDataType> tsDataTypes = new ArrayList<>();
    tsDataTypes.add(TSDataType.INT64);
    tsDataTypes.add(TSDataType.INT32);
    List<TSEncoding> tsEncodings = new ArrayList<>();
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    List<CompressionType> tsCompressionTypes = new ArrayList<>();
    tsCompressionTypes.add(CompressionType.SNAPPY);
    tsCompressionTypes.add(CompressionType.SNAPPY);

    List<Map<String, String>> tagsList = new ArrayList<>();
    Map<String, String> tags = new HashMap<>();
    tags.put("unit", "kg");
    tagsList.add(tags);
    tagsList.add(tags);

    List<Map<String, String>> attributesList = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("minValue", "1");
    attributes.put("maxValue", "100");
    attributesList.add(attributes);
    attributesList.add(attributes);

    List<String> alias = new ArrayList<>();
    alias.add("weight1");
    alias.add("weight2");

    CreateMultiTimeSeriesPlan plan = new CreateMultiTimeSeriesPlan();
    plan.setPaths(paths);
    plan.setDataTypes(tsDataTypes);
    plan.setEncodings(tsEncodings);
    plan.setCompressors(tsCompressionTypes);
    plan.setTags(tagsList);
    plan.setAttributes(attributesList);
    plan.setAlias(alias);

    Log log = new PhysicalPlanLog(plan);
    log.setCurrLogTerm(1);
    log.setCurrLogIndex(2);

    ByteBuffer serialized = log.serialize();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    plan.serialize(dataOutputStream);
    ByteBuffer bufferA = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    ByteBuffer bufferB = ByteBuffer.allocate(bufferA.limit());
    plan.serialize(bufferB);
    bufferB.flip();
    assertEquals(bufferA, bufferB);

    Log parsed = LogParser.getINSTANCE().parse(serialized);
    assertEquals(log, parsed);
  }

  @Test
  public void serdesNodeTest() {
    Node node = new Node("127.0.0.1", 6667, 1, 6535, 4678, "127.0.0.1");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    NodeSerializeUtils.serialize(node, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Node anotherNode = new Node("127.0.0.1", 6667, 1, 6535, 4678, "127.0.0.1");
    NodeSerializeUtils.deserialize(anotherNode, buffer);
    Assert.assertEquals(node, anotherNode);
  }
}
