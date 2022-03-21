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
package org.apache.iotdb.confignode.service.thrift.server;

import org.apache.iotdb.confignode.rpc.thrift.DataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodesInfo;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ConfigNodeRPCServerProcessorTest {

  private final ConfigNodeRPCServerProcessor processor = new ConfigNodeRPCServerProcessor();

  @Test
  public void registerDataNodeTest() throws TException {
    TSStatus status;
    DataNodeRegisterReq registerReq0 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6667));
    DataNodeRegisterReq registerReq1 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6668));
    DataNodeRegisterReq registerReq2 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6669));

    // test success register
    status = processor.registerDataNode(registerReq0);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.registerDataNode(registerReq1);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.registerDataNode(registerReq2);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // test reject register
    status = processor.registerDataNode(registerReq0);
    Assert.assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), status.getCode());

    // test query DataNodeInfo
    DataNodesInfo info = processor.getDataNodesInfo(-1);
    Assert.assertEquals(info.getDataNodesMapSize(), 3);
    List<Map.Entry<Integer, DataNodeInfo>> infoList =
        new ArrayList<>(info.getDataNodesMap().entrySet());
    infoList.sort(Comparator.comparingInt(Map.Entry::getKey));
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(i, infoList.get(i).getValue().getDataNodeID());
      Assert.assertEquals("0.0.0.0", infoList.get(i).getValue().getEndPoint().getIp());
      Assert.assertEquals(6667 + i, infoList.get(i).getValue().getEndPoint().getPort());
    }

    info = processor.getDataNodesInfo(Integer.MAX_VALUE);
    Assert.assertEquals(1, info.getDataNodesMapSize());
    Assert.assertEquals(2, info.getDataNodesMap().get(2).getDataNodeID());
    Assert.assertEquals("0.0.0.0", info.getDataNodesMap().get(2).getEndPoint().getIp());
    Assert.assertEquals(6669, info.getDataNodesMap().get(2).getEndPoint().getPort());
    info = processor.getDataNodesInfo(Integer.MIN_VALUE);
    Assert.assertEquals(1, info.getDataNodesMapSize());
    Assert.assertEquals(0, info.getDataNodesMap().get(0).getDataNodeID());
    Assert.assertEquals("0.0.0.0", info.getDataNodesMap().get(0).getEndPoint().getIp());
    Assert.assertEquals(6667, info.getDataNodesMap().get(0).getEndPoint().getPort());
  }
}
