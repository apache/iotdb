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

package org.apache.iotdb.db.mpp.plan.plan.node.write;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesSchemaNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteTimeSeriesDataNode;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DeleteTimeSeriesSerdeTest {
  @Test
  public void testDataSerializeAndDeserialize() throws IllegalPathException {
    DeleteTimeSeriesDataNode deleteTimeSeriesDataNode =
        new DeleteTimeSeriesDataNode(
            new PlanNodeId("plan node 1"), new PartialPath("root.sg.d.s1"));
    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    deleteTimeSeriesDataNode.serialize(byteBuffer);
    byteBuffer.flip();
    Assert.assertEquals(PlanNodeType.DELETE_TIMESERIES_DATA.ordinal(), byteBuffer.getShort());
    DeleteTimeSeriesDataNode deserialize =
        (DeleteTimeSeriesDataNode) DeleteTimeSeriesDataNode.deserialize(byteBuffer);
    Assert.assertTrue(deserialize.getDeletedPath().matchFullPath(new PartialPath("root.sg.d.s1")));
  }

  @Test
  public void testSchemaSerializeAndDeserialize() throws IllegalPathException {
    DeleteTimeSeriesSchemaNode deleteTimeSeriesDataNode =
        new DeleteTimeSeriesSchemaNode(
            new PlanNodeId("plan node 1"), new PartialPath("root.sg.d.s1"));
    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    deleteTimeSeriesDataNode.serialize(byteBuffer);
    byteBuffer.flip();
    Assert.assertEquals(PlanNodeType.DELETE_TIMESERIES_SCHEMA.ordinal(), byteBuffer.getShort());
    DeleteTimeSeriesSchemaNode deserialize =
        (DeleteTimeSeriesSchemaNode) DeleteTimeSeriesSchemaNode.deserialize(byteBuffer);
    Assert.assertTrue(deserialize.getDeletedPath().matchFullPath(new PartialPath("root.sg.d.s1")));
  }
}
