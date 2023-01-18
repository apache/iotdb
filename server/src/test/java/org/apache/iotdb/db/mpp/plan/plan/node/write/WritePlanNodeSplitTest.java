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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WritePlanNodeSplitTest {

  long prevTimePartitionInterval;

  Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      dataPartitionMap;

  private Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap;

  SeriesPartitionExecutor partitionExecutor;

  String executorClassName;

  int seriesSlotPartitionNum;

  @Before
  public void setUp() {
    prevTimePartitionInterval =
        IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setTimePartitionInterval(100);
    TimePartitionUtils.setTimePartitionInterval(100);

    executorClassName = IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass();
    seriesSlotPartitionNum = IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum();
    partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            executorClassName, seriesSlotPartitionNum);

    initDataPartitionMap();

    initSchemaPartitionMap();
  }

  private void initDataPartitionMap() {
    dataPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        seriesPartitionSlotMap = new HashMap<>();
    // sg1 has 5 data regions
    for (int i = 0; i < seriesSlotPartitionNum; i++) {
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMap = new HashMap<>();
      for (int t = 0; t < 5; t++) {
        long startTime = t * TimePartitionUtils.timePartitionInterval;
        timePartitionSlotMap.put(
            new TTimePartitionSlot(startTime),
            Collections.singletonList(
                new TRegionReplicaSet(
                    new TConsensusGroupId(
                        TConsensusGroupType.DataRegion, getRegionIdByTime(startTime)),
                    null)));
      }

      seriesPartitionSlotMap.put(new TSeriesPartitionSlot(i), timePartitionSlotMap);
    }

    dataPartitionMap.put("root.sg1", seriesPartitionSlotMap);

    // sg2 has 1 data region
    seriesPartitionSlotMap = new HashMap<>();
    for (int i = 0; i < seriesSlotPartitionNum; i++) {
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMap = new HashMap<>();
      for (int t = 0; t < 5; t++) {
        timePartitionSlotMap.put(
            new TTimePartitionSlot(t * TimePartitionUtils.timePartitionInterval),
            Collections.singletonList(
                new TRegionReplicaSet(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 99), null)));
      }

      seriesPartitionSlotMap.put(new TSeriesPartitionSlot(i), timePartitionSlotMap);
    }

    dataPartitionMap.put("root.sg2", seriesPartitionSlotMap);
  }

  private void initSchemaPartitionMap() {
    schemaPartitionMap = new HashMap<>();
    Map<TSeriesPartitionSlot, TRegionReplicaSet> seriesPartitionSlotMap = new HashMap<>();
    for (int i = 0; i < seriesSlotPartitionNum; i++) {
      seriesPartitionSlotMap.put(
          new TSeriesPartitionSlot(i),
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i % 5), null));
    }
    schemaPartitionMap.put("root.sg1", seriesPartitionSlotMap);
  }

  private int getRegionIdByTime(long startTime) {
    return (int) (4 - (startTime / TimePartitionUtils.timePartitionInterval));
  }

  protected DataPartition getDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParamList) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        result = new HashMap<>();

    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParamList) {
      String devicePath = dataPartitionQueryParam.getDevicePath();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesPartitionSlotMap = null;
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesPartitionSlotMapResult = null;
      if (devicePath.startsWith("root.sg1")) {
        seriesPartitionSlotMap = dataPartitionMap.get("root.sg1");
        seriesPartitionSlotMapResult = result.computeIfAbsent("root.sg1", k -> new HashMap<>());
      } else if (devicePath.startsWith("root.sg2")) {
        seriesPartitionSlotMap = dataPartitionMap.get("root.sg2");
        seriesPartitionSlotMapResult = result.computeIfAbsent("root.sg2", k -> new HashMap<>());
      }

      TSeriesPartitionSlot seriesPartitionSlot =
          partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMap =
          seriesPartitionSlotMap.get(seriesPartitionSlot);
      Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMapResult =
          seriesPartitionSlotMapResult.computeIfAbsent(seriesPartitionSlot, k -> new HashMap<>());

      for (TTimePartitionSlot timePartitionSlot :
          dataPartitionQueryParam.getTimePartitionSlotList()) {
        timePartitionSlotMapResult.put(
            timePartitionSlot, timePartitionSlotMap.get(timePartitionSlot));
      }
    }

    return new DataPartition(result, executorClassName, seriesSlotPartitionNum);
  }

  @Test
  public void testSplitInsertTablet() throws IllegalPathException {
    InsertTabletNode insertTabletNode = new InsertTabletNode(new PlanNodeId("plan node 1"));

    insertTabletNode.setDevicePath(new PartialPath("root.sg1.d1"));
    insertTabletNode.setTimes(new long[] {1, 60, 120, 180, 270, 290, 360, 375, 440, 470});
    insertTabletNode.setDataTypes(new TSDataType[] {TSDataType.INT32});
    insertTabletNode.setColumns(new Object[] {new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}});

    DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDevicePath(insertTabletNode.getDevicePath().getFullPath());
    dataPartitionQueryParam.setTimePartitionSlotList(insertTabletNode.getTimePartitionSlots());

    DataPartition dataPartition =
        getDataPartition(Collections.singletonList(dataPartitionQueryParam));
    Analysis analysis = new Analysis();
    analysis.setDataPartitionInfo(dataPartition);

    List<WritePlanNode> insertTabletNodeList = insertTabletNode.splitByPartition(analysis);

    Assert.assertEquals(5, insertTabletNodeList.size());
    for (WritePlanNode insertNode : insertTabletNodeList) {
      InsertTabletNode tabletNode = (InsertTabletNode) insertNode;
      Assert.assertEquals(tabletNode.getTimes().length, 2);
      TConsensusGroupId regionId = tabletNode.getDataRegionReplicaSet().getRegionId();
      Assert.assertEquals(getRegionIdByTime(tabletNode.getMinTime()), regionId.getId());
    }

    insertTabletNode = new InsertTabletNode(new PlanNodeId("plan node 2"));

    insertTabletNode.setDevicePath(new PartialPath("root.sg2.d1"));
    insertTabletNode.setTimes(new long[] {1, 60, 120, 180, 270, 290, 360, 375, 440, 470});
    insertTabletNode.setDataTypes(new TSDataType[] {TSDataType.INT32});
    insertTabletNode.setColumns(new Object[] {new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}});

    dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDevicePath(insertTabletNode.getDevicePath().getFullPath());
    dataPartitionQueryParam.setTimePartitionSlotList(insertTabletNode.getTimePartitionSlots());

    dataPartition = getDataPartition(Collections.singletonList(dataPartitionQueryParam));
    analysis = new Analysis();
    analysis.setDataPartitionInfo(dataPartition);

    insertTabletNodeList = insertTabletNode.splitByPartition(analysis);

    Assert.assertEquals(5, insertTabletNodeList.size());
    for (WritePlanNode insertNode : insertTabletNodeList) {
      Assert.assertEquals(((InsertTabletNode) insertNode).getTimes().length, 2);
    }
  }

  @Test
  public void testInsertMultiTablets() throws IllegalPathException {
    InsertMultiTabletsNode insertMultiTabletsNode =
        new InsertMultiTabletsNode(new PlanNodeId("plan node 3"));

    for (int i = 0; i < 5; i++) {
      InsertTabletNode insertTabletNode = new InsertTabletNode(new PlanNodeId("plan node 3"));
      insertTabletNode.setDevicePath(new PartialPath(String.format("root.sg1.d%d", i)));
      insertTabletNode.setTimes(new long[] {1, 60, 120, 180, 270, 290, 360, 375, 440, 470});
      insertTabletNode.setDataTypes(new TSDataType[] {TSDataType.INT32});
      insertTabletNode.setColumns(
          new Object[] {new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}});
      insertMultiTabletsNode.addInsertTabletNode(insertTabletNode, 2 * i);

      insertTabletNode = new InsertTabletNode(new PlanNodeId("plan node 3"));
      insertTabletNode.setDevicePath(new PartialPath(String.format("root.sg2.d%d", i)));
      insertTabletNode.setTimes(new long[] {1, 60, 120, 180, 270, 290, 360, 375, 440, 470});
      insertTabletNode.setDataTypes(new TSDataType[] {TSDataType.INT32});
      insertTabletNode.setColumns(
          new Object[] {new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}});
      insertMultiTabletsNode.addInsertTabletNode(insertTabletNode, 2 * i);
    }

    List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
    for (InsertTabletNode insertTabletNode : insertMultiTabletsNode.getInsertTabletNodeList()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertTabletNode.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertTabletNode.getTimePartitionSlots());
      dataPartitionQueryParams.add(dataPartitionQueryParam);
    }

    DataPartition dataPartition = getDataPartition(dataPartitionQueryParams);
    Analysis analysis = new Analysis();
    analysis.setDataPartitionInfo(dataPartition);

    List<WritePlanNode> insertTabletNodeList = insertMultiTabletsNode.splitByPartition(analysis);

    Assert.assertEquals(6, insertTabletNodeList.size());
  }

  @Test
  public void testInsertRowsNode() throws IllegalPathException {

    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId("plan node 3"));

    for (int i = 0; i < 5; i++) {
      InsertRowNode insertRowNode = new InsertRowNode(new PlanNodeId("plan node 3"));
      insertRowNode.setDevicePath(new PartialPath(String.format("root.sg1.d%d", i)));
      insertRowNode.setTime(i * TimePartitionUtils.timePartitionInterval);
      insertRowsNode.addOneInsertRowNode(insertRowNode, 2 * i);

      insertRowNode = new InsertRowNode(new PlanNodeId("plan node 3"));
      insertRowNode.setDevicePath(new PartialPath(String.format("root.sg2.d%d", i)));
      insertRowNode.setTime(1);
      insertRowsNode.addOneInsertRowNode(insertRowNode, 2 * i + 1);
    }

    List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
    for (InsertRowNode insertRowNode : insertRowsNode.getInsertRowNodeList()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowNode.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowNode.getTimePartitionSlots());
      dataPartitionQueryParams.add(dataPartitionQueryParam);
    }

    DataPartition dataPartition = getDataPartition(dataPartitionQueryParams);
    Analysis analysis = new Analysis();
    analysis.setDataPartitionInfo(dataPartition);

    List<WritePlanNode> insertTabletNodeList = insertRowsNode.splitByPartition(analysis);

    Assert.assertEquals(6, insertTabletNodeList.size());
  }

  @After
  public void tearDown() {
    TimePartitionUtils.setTimePartitionInterval(prevTimePartitionInterval);
    IoTDBDescriptor.getInstance().getConfig().setTimePartitionInterval(prevTimePartitionInterval);
  }
}
