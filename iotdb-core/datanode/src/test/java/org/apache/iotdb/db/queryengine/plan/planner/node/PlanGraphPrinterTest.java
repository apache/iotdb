/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.node;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.getGraph;
import static org.junit.Assert.assertEquals;

public class PlanGraphPrinterTest {

  @Test
  public void topKNodeTest() throws IllegalPathException {
    OrderByParameter orderByParameter =
        new OrderByParameter(
            Arrays.asList(
                new SortItem("Device", Ordering.ASC), new SortItem("Time", Ordering.DESC)));
    TopKNode topKNode =
        new TopKNode(
            new PlanNodeId("1"), 2, orderByParameter, Arrays.asList("Device", "Time", "s1"));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("2"),
            new OrderByParameter(),
            Collections.emptyList(),
            Collections.emptyMap());

    SeriesScanNode scanNode =
        new SeriesScanNode(new PlanNodeId("3"), new MeasurementPath("root.db.d1.s1"));
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), scanNode);

    topKNode.addChild(deviceViewNode);

    List<String> result = getGraph(topKNode);
    assertEquals(19, result.size());
    assertEquals("│TopK-1                                 │", result.get(1));
    assertEquals("            │DeviceView-2  │             ", result.get(8));
    assertEquals("        │SeriesScan-3           │        ", result.get(14));
  }
}
