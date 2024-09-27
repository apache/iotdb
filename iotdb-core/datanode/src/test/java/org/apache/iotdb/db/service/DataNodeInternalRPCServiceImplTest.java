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

package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;

import org.apache.ratis.util.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataNodeInternalRPCServiceImplTest {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  DataNodeInternalRPCServiceImpl dataNodeInternalRPCServiceImpl;
  private static final int dataNodeId = 0;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, MetadataException {
    // In standalone mode, we need to set dataNodeId to 0 for RaftPeerId in RatisConsensus
    conf.setDataNodeId(dataNodeId);

    SchemaEngine.getInstance().init();
    SchemaEngine.getInstance()
        .createSchemaRegion(new PartialPath("root.ln"), new SchemaRegionId(0));
    DataRegionConsensusImpl.getInstance().start();
    SchemaRegionConsensusImpl.getInstance().start();
    DataNodeRegionManager.getInstance().init();
  }

  @Before
  public void setUp() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .createLocalPeer(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(regionReplicaSet.getRegionId()),
            genSchemaRegionPeerList(regionReplicaSet));
    dataNodeInternalRPCServiceImpl = new DataNodeInternalRPCServiceImpl();
  }

  @After
  public void tearDown() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .deleteLocalPeer(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(regionReplicaSet.getRegionId()));
    FileUtils.deleteFully(new File(conf.getConsensusDir()));
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException, StorageEngineException {
    DataNodeRegionManager.getInstance().clear();
    DataRegionConsensusImpl.getInstance().stop();
    SchemaRegionConsensusImpl.getInstance().stop();
    SchemaEngine.getInstance().clear();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateTimeseries() throws MetadataException {
    CreateTimeSeriesNode createTimeSeriesNode =
        new CreateTimeSeriesNode(
            new PlanNodeId("0"),
            new MeasurementPath("root.ln.wf01.wt01.status"),
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

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();

    // serialize planNode
    ByteBuffer byteBuffer = createTimeSeriesNode.serializeToByteBuffer();

    // put serialized planNode to TSendPlanNodeReq
    TSendSinglePlanNodeReq request = new TSendSinglePlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendBatchPlanNodeResp response =
        dataNodeInternalRPCServiceImpl.sendBatchPlanNode(
            new TSendBatchPlanNodeReq(Collections.singletonList(request)));

    Assert.assertTrue(response.getResponses().get(0).accepted);
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

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    // serialize planNode
    ByteBuffer byteBuffer = createAlignedTimeSeriesNode.serializeToByteBuffer();

    // put serialized planNode to TSendPlanNodeReq
    TSendSinglePlanNodeReq request = new TSendSinglePlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendBatchPlanNodeResp response =
        dataNodeInternalRPCServiceImpl.sendBatchPlanNode(
            new TSendBatchPlanNodeReq(Collections.singletonList(request)));

    Assert.assertTrue(response.getResponses().get(0).accepted);
  }

  @Test
  public void testCreateMultiTimeSeries() throws MetadataException {
    CreateMultiTimeSeriesNode createMultiTimeSeriesNode =
        new CreateMultiTimeSeriesNode(
            new PlanNodeId("0"),
            new ArrayList<MeasurementPath>() {
              {
                add(new MeasurementPath("root.ln.d3.s1"));
                add(new MeasurementPath("root.ln.d3.s2"));
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

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();

    // serialize planNode
    ByteBuffer byteBuffer = createMultiTimeSeriesNode.serializeToByteBuffer();

    // put serialized planNode to TSendPlanNodeReq
    TSendSinglePlanNodeReq request = new TSendSinglePlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendBatchPlanNodeResp response =
        dataNodeInternalRPCServiceImpl.sendBatchPlanNode(
            new TSendBatchPlanNodeReq(Collections.singletonList(request)));

    Assert.assertTrue(response.getResponses().get(0).accepted);
  }

  private TRegionReplicaSet genRegionReplicaSet() {
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
    return new TRegionReplicaSet(
        new TConsensusGroupId(TConsensusGroupType.SchemaRegion, conf.getDataNodeId()),
        dataNodeList);
  }

  private List<Peer> genSchemaRegionPeerList(TRegionReplicaSet regionReplicaSet) {
    List<Peer> peerList = new ArrayList<>();
    for (TDataNodeLocation node : regionReplicaSet.getDataNodeLocations()) {
      peerList.add(
          new Peer(
              new SchemaRegionId(regionReplicaSet.getRegionId().getId()),
              node.getDataNodeId(),
              node.getSchemaRegionConsensusEndPoint()));
    }
    return peerList;
  }
}
