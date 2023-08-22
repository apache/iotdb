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

package org.apache.iotdb.db.queryengine.plan.plan;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class FEPlanUtil {

  private FEPlanUtil() {
    // Util class does not need a Construct Method
  }

  protected static LocalExecutionPlanContext createLocalExecutionPlanContext(
      final TypeProvider typeProvider) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    return new LocalExecutionPlanContext(
        typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  /**
   * This method will init a timeJoinNode with @childNum seriesScanNode as children.
   *
   * @param childNum the number of children
   * @return a timeJoinNode with @childNum seriesScanNode as children
   */
  protected static TimeJoinNode initTimeJoinNode(
      final TypeProvider typeProvider, final int childNum) throws IllegalPathException {
    TimeJoinNode timeJoinNode = new TimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.ASC);
    for (int i = 0; i < childNum; i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      timeJoinNode.addChild(seriesScanNode);
    }
    return timeJoinNode;
  }

  /**
   * This method will init a DeviceViewNode with @childNum alignedSeriesScanNode as children.
   *
   * @param childNum the number of children
   * @return a DeviceViewNode with @childNum alignedSeriesScanNode as children
   */
  protected static DeviceViewNode initDeviceViewNode(
      final TypeProvider typeProvider, final int childNum) throws IllegalPathException {
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("DeviceViewNode"), null, Collections.EMPTY_LIST, Collections.EMPTY_MAP);
    for (int i = 0; i < childNum; i++) {
      AlignedSeriesScanNode alignedSeriesScanNode =
          new AlignedSeriesScanNode(
              new PlanNodeId(String.format("AlignedSeriesScanNode%d", i)),
              new AlignedPath(String.format("root.sg.d%d", i), "s1"));
      deviceViewNode.addChild(alignedSeriesScanNode);
    }
    return deviceViewNode;
  }
}
