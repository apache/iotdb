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

package org.apache.iotdb.db.queryengine.plan.planner.node.pipe;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class PipeEnrichedInsertNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    InsertRowNode insertRowNode = getInsertRowNode();
    PipeEnrichedInsertNode pipeEnrichedInsertNode = new PipeEnrichedInsertNode(insertRowNode);

    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    pipeEnrichedInsertNode.serialize(byteBuffer);
    byteBuffer.flip();

    Assert.assertEquals(
        PlanNodeType.PIPE_ENRICHED_INSERT_DATA.getNodeType(), byteBuffer.getShort());
    Assert.assertEquals(PlanNodeType.INSERT_ROW.getNodeType(), byteBuffer.getShort());

    Assert.assertEquals(insertRowNode, InsertRowNode.deserialize(byteBuffer));
  }

  private InsertRowNode getInsertRowNode() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
        };

    Object[] columns = new Object[5];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000l;
    columns[3] = 100;
    columns[4] = false;

    return new InsertRowNode(
        new PlanNodeId("plannode 1"),
        new PartialPath("root.isp.d1"),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5"},
        dataTypes,
        time,
        columns,
        false);
  }
}
