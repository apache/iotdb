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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class LimitNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    SeriesScanNode seriesScanNode =
        new SeriesScanNode(
            new PlanNodeId("TestSeriesScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Ordering.DESC,
            new GroupByFilter(1, 2, 3, 4),
            null,
            100,
            100,
            null);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("TestOffsetNode"), seriesScanNode, 2);
    LimitNode limitNode = new LimitNode(new PlanNodeId("TestLimitNode"), offsetNode, 3);

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    limitNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), limitNode);
  }
}
