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

package org.apache.iotdb.db.queryengine.plan.planner.node.write;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

public class ContinuousSameSearchIndexSeparatorNodeSerDeTest {
  @Test
  public void testSerializeAndDeserializeForWAL() throws Exception {
    ContinuousSameSearchIndexSeparatorNode node =
        new ContinuousSameSearchIndexSeparatorNode(new PlanNodeId("???"));

    int serializedSize = node.serializedSize();

    byte[] bytes = new byte[serializedSize];
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.wrap(bytes));

    node.serializeToWAL(walBuffer);
    Assert.assertFalse(walBuffer.getBuffer().hasRemaining());

    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    Assert.assertEquals(
        PlanNodeType.CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR.getNodeType(),
        dataInputStream.readShort());

    ContinuousSameSearchIndexSeparatorNode.deserializeFromWAL(dataInputStream);
  }
}
