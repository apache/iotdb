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
package org.apache.iotdb.db.queryengine.plan.plan.node.process;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class FilterNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(new PlanNodeId("TestTimeJoinNode"), Ordering.ASC);
    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("TestFilterNode"),
            fullOuterTimeJoinNode,
            new Expression[] {new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))},
            new GreaterThanExpression(
                new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                new ConstantOperand(TSDataType.INT64, "100")),
            false,
            ZoneId.systemDefault(),
            Ordering.ASC);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    filterNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), filterNode);
  }
}
