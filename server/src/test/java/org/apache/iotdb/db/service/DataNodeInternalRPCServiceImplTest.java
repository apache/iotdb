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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.service.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.ratis.util.FileUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataNodeInternalRPCServiceImplTest {
  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  DataNodeInternalRPCServiceImpl dataNodeInternalRPCServiceImpl;
  static LocalConfigNode configNode;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, MetadataException {
    IoTDB.configManager.init();
    configNode = LocalConfigNode.getInstance();
    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath("root.ln"));
    DataRegionConsensusImpl.setupAndGetInstance().start();
    SchemaRegionConsensusImpl.setupAndGetInstance().start();
  }

  @Before
  public void setUp() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .createPeer(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(regionReplicaSet.getRegionId()),
            genSchemaRegionPeerList(regionReplicaSet));
    dataNodeInternalRPCServiceImpl = new DataNodeInternalRPCServiceImpl();
  }

  @After
  public void tearDown() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .deletePeer(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(regionReplicaSet.getRegionId()));
    FileUtils.deleteFully(new File(conf.getConsensusDir()));
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException, StorageEngineException {
    DataRegionConsensusImpl.getInstance().stop();
    SchemaRegionConsensusImpl.getInstance().stop();
    IoTDB.configManager.clear();
    EnvironmentUtils.cleanEnv();
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

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();

    // serialize planNode
    ByteBuffer byteBuffer = createTimeSeriesNode.serializeToByteBuffer();

    // put serialized planNode to TSendPlanNodeReq
    TSendPlanNodeReq request = new TSendPlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendPlanNodeResp response = dataNodeInternalRPCServiceImpl.sendPlanNode(request);

    Assert.assertTrue(response.accepted);
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
    TSendPlanNodeReq request = new TSendPlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendPlanNodeResp response = dataNodeInternalRPCServiceImpl.sendPlanNode(request);

    Assert.assertTrue(response.accepted);
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

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();

    // serialize planNode
    ByteBuffer byteBuffer = createMultiTimeSeriesNode.serializeToByteBuffer();

    // put serialized planNode to TSendPlanNodeReq
    TSendPlanNodeReq request = new TSendPlanNodeReq();
    TPlanNode tPlanNode = new TPlanNode();
    tPlanNode.setBody(byteBuffer);
    request.setPlanNode(tPlanNode);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());

    // Use consensus layer to execute request
    TSendPlanNodeResp response = dataNodeInternalRPCServiceImpl.sendPlanNode(request);

    Assert.assertTrue(response.accepted);
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
        new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0), dataNodeList);
  }

  private List<Peer> genSchemaRegionPeerList(TRegionReplicaSet regionReplicaSet) {
    List<Peer> peerList = new ArrayList<>();
    for (TDataNodeLocation node : regionReplicaSet.getDataNodeLocations()) {
      peerList.add(
          new Peer(
              new SchemaRegionId(regionReplicaSet.getRegionId().getId()),
              node.getSchemaRegionConsensusEndPoint()));
    }
    return peerList;
  }
}
