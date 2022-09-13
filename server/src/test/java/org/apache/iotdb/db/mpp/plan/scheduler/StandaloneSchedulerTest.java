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

package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.DataRegionTest;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.localconfignode.LocalDataPartitionTable;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.QueryState;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class StandaloneSchedulerTest {
  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  static LocalConfigNode configNode;

  @Before
  public void setUp() throws Exception {
    conf.setMppMode(true);
    conf.setDataNodeId(0);

    configNode = LocalConfigNode.getInstance();
    configNode.init();
    WALManager.getInstance().start();
    FlushManager.getInstance().start();
    StorageEngineV2.getInstance().start();
    LocalDataPartitionTable.DataRegionIdGenerator.getInstance().reset();
  }

  @After
  public void tearDown() throws Exception {
    configNode.clear();
    WALManager.getInstance().stop();
    StorageEngineV2.getInstance().stop();
    FlushManager.getInstance().stop();
    EnvironmentUtils.cleanAllDir();
    conf.setDataNodeId(-1);
    conf.setMppMode(false);
  }

  @Test
  public void testCreateTimeseries() throws MetadataException {
    CreateTimeSeriesNode createTimeSeriesNode =
        new CreateTimeSeriesNode(
            new PlanNodeId("0"),
            new PartialPath("root.ln.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            new HashMap<String, String>() {
              {
                put("MAX_POINT_NUMBER", "3");
              }
            },
            new HashMap<String, String>() {
              {
                put("tag1", "v1");
                put("tag2", "v2");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
                put("attr2", "a2");
              }
            },
            "meter1");

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet(TConsensusGroupType.SchemaRegion);
    PlanFragment planFragment = new PlanFragment(new PlanFragmentId("2", 3), createTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE,
            conf.getQueryTimeoutThreshold());
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath("root.ln.wf01.wt01.status"));
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("query1"),
            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
            new TEndPoint(),
            new TEndPoint());
    ExecutorService executor = IoTDBThreadPoolFactory.newSingleThreadExecutor("Test");
    QueryStateMachine stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    Assert.assertFalse(stateMachine.getState().isDone());
    // execute request
    StandaloneScheduler standaloneScheduler =
        new StandaloneScheduler(
            context,
            stateMachine,
            Collections.singletonList(fragmentInstance),
            QueryType.WRITE,
            null,
            null);
    try {
      standaloneScheduler.start();
      Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      standaloneScheduler.stop();
    }
  }

  @Test
  public void testCreateAlignedTimeseries() throws MetadataException {
    CreateAlignedTimeSeriesNode createAlignedTimeSeriesNode =
        new CreateAlignedTimeSeriesNode(
            new PlanNodeId("0"),
            new PartialPath("root.ln.wf01.GPS"),
            new ArrayList<String>() {
              {
                add("latitude");
                add("longitude");
              }
            },
            new ArrayList<TSDataType>() {
              {
                add(TSDataType.FLOAT);
                add(TSDataType.FLOAT);
              }
            },
            new ArrayList<TSEncoding>() {
              {
                add(TSEncoding.PLAIN);
                add(TSEncoding.PLAIN);
              }
            },
            new ArrayList<CompressionType>() {
              {
                add(CompressionType.SNAPPY);
                add(CompressionType.SNAPPY);
              }
            },
            new ArrayList<String>() {
              {
                add("meter1");
                add(null);
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("tag1", "t1");
                      }
                    });
                add(null);
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("tag1", "t1");
                      }
                    });
                add(null);
              }
            });

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet(TConsensusGroupType.SchemaRegion);
    PlanFragment planFragment =
        new PlanFragment(new PlanFragmentId("2", 3), createAlignedTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE,
            conf.getQueryTimeoutThreshold());
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath("root.ln.wf01.GPS"));
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("query1"),
            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
            new TEndPoint(),
            new TEndPoint());
    ExecutorService executor = IoTDBThreadPoolFactory.newSingleThreadExecutor("Test");
    QueryStateMachine stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    Assert.assertFalse(stateMachine.getState().isDone());
    // execute request
    StandaloneScheduler standaloneScheduler =
        new StandaloneScheduler(
            context,
            stateMachine,
            Collections.singletonList(fragmentInstance),
            QueryType.WRITE,
            null,
            null);
    try {
      standaloneScheduler.start();
      Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      standaloneScheduler.stop();
    }
  }

  @Test
  public void testCreateMultiTimeSeries() throws MetadataException {
    CreateMultiTimeSeriesNode createMultiTimeSeriesNode =
        new CreateMultiTimeSeriesNode(
            new PlanNodeId("0"),
            new ArrayList<PartialPath>() {
              {
                add(new PartialPath("root.ln.d3.s1"));
                add(new PartialPath("root.ln.d3.s2"));
              }
            },
            new ArrayList<TSDataType>() {
              {
                add(TSDataType.FLOAT);
                add(TSDataType.FLOAT);
              }
            },
            new ArrayList<TSEncoding>() {
              {
                add(TSEncoding.PLAIN);
                add(TSEncoding.PLAIN);
              }
            },
            new ArrayList<CompressionType>() {
              {
                add(CompressionType.SNAPPY);
                add(CompressionType.SNAPPY);
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("MAX_POINT_NUMBER", "3");
                      }
                    });
                add(null);
              }
            },
            new ArrayList<String>() {
              {
                add("meter1");
                add(null);
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("tag1", "t1");
                      }
                    });
                add(null);
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("tag1", "t1");
                      }
                    });
                add(null);
              }
            });

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet(TConsensusGroupType.SchemaRegion);
    PlanFragment planFragment =
        new PlanFragment(new PlanFragmentId("2", 3), createMultiTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE,
            conf.getQueryTimeoutThreshold());
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath("root.ln.d3"));
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("query1"),
            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
            new TEndPoint(),
            new TEndPoint());
    ExecutorService executor = IoTDBThreadPoolFactory.newSingleThreadExecutor("Test");
    QueryStateMachine stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    Assert.assertFalse(stateMachine.getState().isDone());
    // execute request
    StandaloneScheduler standaloneScheduler =
        new StandaloneScheduler(
            context,
            stateMachine,
            Collections.singletonList(fragmentInstance),
            QueryType.WRITE,
            null,
            null);
    try {
      standaloneScheduler.start();
      Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      standaloneScheduler.stop();
    }
  }

  @Test
  public void testInsertRow() throws DataRegionException, MetadataException {
    String deviceId = "root.vehicle.d0";
    String measurementId = "s0";
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    InsertRowNode insertRowNode = DataRegionTest.buildInsertRowNodeByTSRecord(record);

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet(TConsensusGroupType.DataRegion);
    PlanFragment planFragment = new PlanFragment(new PlanFragmentId("2", 3), insertRowNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE,
            conf.getQueryTimeoutThreshold());
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath(deviceId));
    configNode.getBelongedDataRegionIdWithAutoCreate(new PartialPath(deviceId));
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("query1"),
            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
            new TEndPoint(),
            new TEndPoint());
    ExecutorService executor = IoTDBThreadPoolFactory.newSingleThreadExecutor("Test");
    QueryStateMachine stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    Assert.assertFalse(stateMachine.getState().isDone());
    // execute request
    StandaloneScheduler standaloneScheduler =
        new StandaloneScheduler(
            context,
            stateMachine,
            Collections.singletonList(fragmentInstance),
            QueryType.WRITE,
            null,
            null);
    try {
      standaloneScheduler.start();
      Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      standaloneScheduler.stop();
    }
  }

  @Test
  public void testInsertTablet() throws DataRegionException, MetadataException {
    PartialPath deviceId = new PartialPath("root.vehicle.d0");
    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.INT32;
    dataTypes[1] = TSDataType.INT64;

    MeasurementSchema[] measurementSchemas = new MeasurementSchema[2];
    measurementSchemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    measurementSchemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            deviceId,
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet(TConsensusGroupType.DataRegion);
    PlanFragment planFragment = new PlanFragment(new PlanFragmentId("2", 3), insertTabletNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE,
            conf.getQueryTimeoutThreshold());
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    configNode.getBelongedSchemaRegionIdWithAutoCreate(deviceId);
    configNode.getBelongedDataRegionIdWithAutoCreate(deviceId);
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("query1"),
            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
            new TEndPoint(),
            new TEndPoint());
    ExecutorService executor = IoTDBThreadPoolFactory.newSingleThreadExecutor("Test");
    QueryStateMachine stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    Assert.assertFalse(stateMachine.getState().isDone());
    // execute request
    StandaloneScheduler standaloneScheduler =
        new StandaloneScheduler(
            context,
            stateMachine,
            Collections.singletonList(fragmentInstance),
            QueryType.WRITE,
            null,
            null);
    try {
      standaloneScheduler.start();
      Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      standaloneScheduler.stop();
    }
  }

  private TRegionReplicaSet genRegionReplicaSet(TConsensusGroupType type) {
    List<TDataNodeLocation> dataNodeList = new ArrayList<>();
    dataNodeList.add(
        new TDataNodeLocation()
            .setClientRpcEndPoint(new TEndPoint(conf.getRpcAddress(), conf.getRpcPort()))
            .setInternalEndPoint(new TEndPoint(conf.getInternalAddress(), conf.getInternalPort()))
            .setMPPDataExchangeEndPoint(
                new TEndPoint(conf.getInternalAddress(), conf.getMppDataExchangePort()))
            .setDataRegionConsensusEndPoint(
                new TEndPoint(conf.getInternalAddress(), conf.getDataRegionConsensusPort()))
            .setSchemaRegionConsensusEndPoint(
                new TEndPoint(conf.getInternalAddress(), conf.getSchemaRegionConsensusPort())));

    // construct fragmentInstance
    return new TRegionReplicaSet(new TConsensusGroupId(type, 0), dataNodeList);
  }
}
