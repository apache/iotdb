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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.object.MPPObjectPool;
import org.apache.iotdb.db.mpp.common.object.entry.SchemaFetchObjectEntry;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class SchemaFetchScanOperatorTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSchemaFetchResult() throws Exception {
    String queryId = "1";
    ISchemaRegion schemaRegion = prepareSchemaRegion();

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.**.status"));
    patternTree.appendPathPattern(new PartialPath("root.**.s1"));
    patternTree.constructTree();

    SchemaFetchScanOperator schemaFetchScanOperator =
        new SchemaFetchScanOperator(
            null,
            prepareOperatorContext(queryId),
            patternTree,
            Collections.emptyMap(),
            schemaRegion,
            false);

    Assert.assertTrue(schemaFetchScanOperator.hasNext());

    TsBlock tsBlock = schemaFetchScanOperator.next();

    Assert.assertFalse(schemaFetchScanOperator.hasNext());

    SchemaFetchObjectEntry objectEntry =
        MPPObjectPool.getInstance().getQueryObjectPool(queryId).get(tsBlock.getColumn(0).getInt(0));
    Assert.assertFalse(objectEntry.isReadingStorageGroupInfo());
    ISchemaTree schemaTree = objectEntry.getSchemaTree();

    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(
            new PartialPath("root.sg.d2.a"), Arrays.asList("s1", "status"));
    Assert.assertTrue(deviceSchemaInfo.isAligned());
    List<MeasurementSchema> measurementSchemaList = deviceSchemaInfo.getMeasurementSchemaList();
    Assert.assertEquals(2, measurementSchemaList.size());
    Assert.assertEquals(
        Arrays.asList("s1", "s2"),
        measurementSchemaList.stream()
            .map(MeasurementSchema::getMeasurementId)
            .sorted()
            .collect(Collectors.toList()));

    Pair<List<MeasurementPath>, Integer> pair =
        schemaTree.searchMeasurementPaths(new PartialPath("root.sg.**.status"), 0, 0, false);
    Assert.assertEquals(3, pair.left.size());
    Assert.assertEquals(
        Arrays.asList("root.sg.d1.s2", "root.sg.d2.a.s2", "root.sg.d2.s2"),
        pair.left.stream().map(MeasurementPath::getFullPath).collect(Collectors.toList()));
  }

  private OperatorContext prepareOperatorContext(String queryId) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, SchemaFetchScanOperator.class.getSimpleName());
      fragmentInstanceContext.registerQueryObjectPool();
      return fragmentInstanceContext.getOperatorContexts().get(0);
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private ISchemaRegion prepareSchemaRegion() throws Exception {
    SchemaEngine schemaEngine = SchemaEngine.getInstance();
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    schemaEngine.createSchemaRegion(new PartialPath("root.sg"), schemaRegionId);
    ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.d1.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null,
            null,
            null,
            null);
    schemaRegion.createTimeseries(createTimeSeriesPlan, -1);

    createTimeSeriesPlan.setPath(new PartialPath("root.sg.d2.s1"));
    schemaRegion.createTimeseries(createTimeSeriesPlan, -1);

    createTimeSeriesPlan.setAlias("status");
    createTimeSeriesPlan.setPath(new PartialPath("root.sg.d1.s2"));
    schemaRegion.createTimeseries(createTimeSeriesPlan, -1);

    createTimeSeriesPlan.setPath(new PartialPath("root.sg.d2.s2"));
    schemaRegion.createTimeseries(createTimeSeriesPlan, -1);

    CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
        new CreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.d2.a"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.INT32),
            Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.UNCOMPRESSED, CompressionType.UNCOMPRESSED),
            Arrays.asList(null, "status"),
            Collections.emptyList(),
            Collections.emptyList());

    schemaRegion.createAlignedTimeSeries(createAlignedTimeSeriesPlan);

    return schemaRegion;
  }
}
