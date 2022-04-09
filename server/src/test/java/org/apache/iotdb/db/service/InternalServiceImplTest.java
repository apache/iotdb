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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.DataNodeLocation;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.sql.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InternalServiceImplTest {

  @Before
  public void setUp() throws Exception {
    IoTDB.configManager.init();
  }

  @After
  public void tearDown() throws Exception {
    IoTDB.configManager.clear();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void createTimeseriesTest() throws IllegalPathException, TException {
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

    List<DataNodeLocation> dataNodeList = new ArrayList<>();

    dataNodeList.add(new DataNodeLocation(6667, new Endpoint("0.0.0.0", 6667)));
    dataNodeList.add(new DataNodeLocation(6668, new Endpoint("0.0.0.0", 6668)));
    dataNodeList.add(new DataNodeLocation(6669, new Endpoint("0.0.0.0", 6669)));

    // construct fragmentInstance
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    RegionReplicaSet regionReplicaSet = new RegionReplicaSet(schemaRegionId, dataNodeList);
    PlanFragment planFragment = new PlanFragment(new PlanFragmentId("2", 3), createTimeSeriesNode);
    FragmentInstance fragmentInstance = new FragmentInstance(planFragment, 4);
    fragmentInstance.setRegionReplicaSet(regionReplicaSet);

    // serialize fragmentInstance
    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);
    fragmentInstance.serializeRequest(byteBuffer);
    byteBuffer.flip();

    // put serialized fragmentInstance to TSendFragmentInstanceReq
    TSendFragmentInstanceReq request = new TSendFragmentInstanceReq();
    TFragmentInstance tFragmentInstance = new TFragmentInstance();
    tFragmentInstance.setBody(byteBuffer);
    request.setFragmentInstance(tFragmentInstance);

    // Use consensus layer to execute request
    InternalServiceImpl internalServiceImpl = new InternalServiceImpl();
    TSendFragmentInstanceResp response = internalServiceImpl.sendFragmentInstance(request);

    Assert.assertTrue(response.accepted);
  }
}
