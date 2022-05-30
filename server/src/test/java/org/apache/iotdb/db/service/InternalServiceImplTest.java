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
import org.apache.iotdb.db.metadata.utils.TimeseriesVersionUtil;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.service.thrift.impl.InternalServiceImpl;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;

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

public class InternalServiceImplTest {
  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  InternalServiceImpl internalServiceImpl;
  static LocalConfigNode configNode;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, MetadataException {
    IoTDB.configManager.init();
    configNode = LocalConfigNode.getInstance();
    configNode.getBelongedSchemaRegionIdWithAutoCreate(new PartialPath("root.ln"));
    DataRegionConsensusImpl.getInstance().start();
    SchemaRegionConsensusImpl.getInstance().start();
  }

  @Before
  public void setUp() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .addConsensusGroup(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(regionReplicaSet.getRegionId()),
            genSchemaRegionPeerList(regionReplicaSet));
    internalServiceImpl = new InternalServiceImpl();
  }

  @After
  public void tearDown() throws Exception {
    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    SchemaRegionConsensusImpl.getInstance()
        .removeConsensusGroup(
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
            "meter1",
            TimeseriesVersionUtil.generateVersion());

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    PlanFragment planFragment = new PlanFragment(new PlanFragmentId("2", 3), createTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE);
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    // serialize fragmentInstance
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();

    // put serialized fragmentInstance to TSendFragmentInstanceReq
    TSendFragmentInstanceReq request = new TSendFragmentInstanceReq();
    TFragmentInstance tFragmentInstance = new TFragmentInstance();
    tFragmentInstance.setBody(byteBuffer);
    request.setFragmentInstance(tFragmentInstance);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());
    request.setQueryType(QueryType.WRITE.toString());

    // Use consensus layer to execute request
    TSendFragmentInstanceResp response = internalServiceImpl.sendFragmentInstance(request);

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
            },
            new ArrayList<String>() {
              {
                add(TimeseriesVersionUtil.generateVersion());
                add(TimeseriesVersionUtil.generateVersion());
              }
            });

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    PlanFragment planFragment =
        new PlanFragment(new PlanFragmentId("2", 3), createAlignedTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE);
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    // serialize fragmentInstance
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();

    // put serialized fragmentInstance to TSendFragmentInstanceReq
    TSendFragmentInstanceReq request = new TSendFragmentInstanceReq();
    TFragmentInstance tFragmentInstance = new TFragmentInstance();
    tFragmentInstance.setBody(byteBuffer);
    request.setFragmentInstance(tFragmentInstance);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());
    request.setQueryType(QueryType.WRITE.toString());

    // Use consensus layer to execute request
    TSendFragmentInstanceResp response = internalServiceImpl.sendFragmentInstance(request);

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
            },
            new ArrayList<String>() {
              {
                add(TimeseriesVersionUtil.generateVersion());
                add(TimeseriesVersionUtil.generateVersion());
              }
            });

    TRegionReplicaSet regionReplicaSet = genRegionReplicaSet();
    PlanFragment planFragment =
        new PlanFragment(new PlanFragmentId("2", 3), createMultiTimeSeriesNode);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment,
            planFragment.getId().genFragmentInstanceId(),
            new GroupByFilter(1, 2, 3, 4),
            QueryType.WRITE);
    fragmentInstance.setDataRegionAndHost(regionReplicaSet);

    // serialize fragmentInstance
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();

    // put serialized fragmentInstance to TSendFragmentInstanceReq
    TSendFragmentInstanceReq request = new TSendFragmentInstanceReq();
    TFragmentInstance tFragmentInstance = new TFragmentInstance();
    tFragmentInstance.setBody(byteBuffer);
    request.setFragmentInstance(tFragmentInstance);
    request.setConsensusGroupId(regionReplicaSet.getRegionId());
    request.setQueryType(QueryType.WRITE.toString());

    // Use consensus layer to execute request
    TSendFragmentInstanceResp response = internalServiceImpl.sendFragmentInstance(request);

    Assert.assertTrue(response.accepted);
  }

  private TRegionReplicaSet genRegionReplicaSet() {
    List<TDataNodeLocation> dataNodeList = new ArrayList<>();
    dataNodeList.add(
        new TDataNodeLocation()
            .setExternalEndPoint(new TEndPoint(conf.getRpcAddress(), conf.getRpcPort()))
            .setInternalEndPoint(new TEndPoint(conf.getInternalIp(), conf.getInternalPort()))
            .setDataBlockManagerEndPoint(
                new TEndPoint(conf.getInternalIp(), conf.getDataBlockManagerPort()))
            .setDataRegionConsensusEndPoint(
                new TEndPoint(conf.getInternalIp(), conf.getDataRegionConsensusPort()))
            .setSchemaRegionConsensusEndPoint(
                new TEndPoint(conf.getInternalIp(), conf.getSchemaRegionConsensusPort())));

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
