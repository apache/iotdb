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
package org.apache.iotdb.db.mpp.sql.plan.node.process;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.sql.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class FillNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("TestTimeJoinNode"), OrderBy.TIMESTAMP_ASC);
    FillNode fillNode =
        new FillNode(
            new PlanNodeId("TestFillNode"),
            timeJoinNode,
            Collections.singletonList(
                new FillDescriptor(
                    FillPolicy.PREVIOUS, new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")))));

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fillNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), fillNode);
  }
}
