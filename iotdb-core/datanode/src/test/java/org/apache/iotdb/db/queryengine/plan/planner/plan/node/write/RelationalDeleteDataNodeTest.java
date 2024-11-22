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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.And;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.SegmentExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;

import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class RelationalDeleteDataNodeTest {

  @Test
  public void testSerialization() throws IOException {
    RelationalDeleteDataNode relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId("testPlan"),
            Arrays.asList(
                new TableDeletionEntry(
                    new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10)),
                new TableDeletionEntry(
                    new DeletionPredicate(
                        "table2",
                        new FullExactMatch(
                            Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"}))),
                    new TimeRange(0, 20)),
                new TableDeletionEntry(
                    new DeletionPredicate("table3", new SegmentExactMatch("id1", 1)),
                    new TimeRange(1, 5)),
                new TableDeletionEntry(
                    new DeletionPredicate(
                        "table1",
                        new And(
                            new FullExactMatch(
                                Factory.DEFAULT_FACTORY.create(new String[] {"id1", "id2"})),
                            new SegmentExactMatch("id1", 1))),
                    new TimeRange(623, 1677)),
                new TableDeletionEntry(
                    new DeletionPredicate("table1", new NOP(), Arrays.asList("s1", "s2")),
                    new TimeRange(0, 10))));
    relationalDeleteDataNode.setProgressIndex(new IoTProgressIndex(0, 1L));

    ByteBuffer buffer = ByteBuffer.allocate(relationalDeleteDataNode.serializedSize());
    relationalDeleteDataNode.serialize(buffer);
    buffer.flip();
    assertEquals(relationalDeleteDataNode, PlanNodeType.deserialize(buffer));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    relationalDeleteDataNode.serialize(new DataOutputStream(byteArrayOutputStream));
    assertEquals(
        relationalDeleteDataNode,
        PlanNodeType.deserialize(ByteBuffer.wrap(byteArrayOutputStream.toByteArray())));

    buffer = ByteBuffer.allocate(relationalDeleteDataNode.serializedSize());
    WALByteBufferForTest walByteBufferForTest = new WALByteBufferForTest(buffer);
    relationalDeleteDataNode.serializeToWAL(walByteBufferForTest);
    buffer.flip();
    PlanNode planNode = PlanNodeType.deserializeFromWAL(buffer);
    // plan node id is not serialized to WAL, manually set it to pass comparison
    planNode.setPlanNodeId(relationalDeleteDataNode.getPlanNodeId());
    assertEquals(relationalDeleteDataNode, planNode);

    buffer = relationalDeleteDataNode.serializeToDAL();
    AbstractDeleteDataNode deserialized = DeleteNodeType.deserializeFromDAL(buffer);
    // plan node id is not serialized to DAL, manually set it to pass comparison
    deserialized.setPlanNodeId(relationalDeleteDataNode.getPlanNodeId());
    assertEquals(relationalDeleteDataNode, deserialized);
    assertEquals(relationalDeleteDataNode.getProgressIndex(), deserialized.getProgressIndex());
  }
}
