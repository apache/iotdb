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
package org.apache.iotdb.db.queryengine.plan.planner.node.process;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.literal.LongLiteral;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class FillNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(new PlanNodeId("TestTimeJoinNode"), Ordering.ASC);
    FillNode fillNode =
        new FillNode(
            new PlanNodeId("TestFillNode"),
            fullOuterTimeJoinNode,
            new FillDescriptor(FillPolicy.CONSTANT, new LongLiteral("100"), null),
            Ordering.ASC);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fillNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), fillNode);
  }
}
