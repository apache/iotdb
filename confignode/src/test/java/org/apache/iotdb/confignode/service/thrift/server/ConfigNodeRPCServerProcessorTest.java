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

import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.SetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessage;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ConfigNodeRPCServerProcessorTest {

  @Test
  public void registerDataNodeTest() throws TException, IOException {
    ConfigNodeRPCServerProcessor processor = new ConfigNodeRPCServerProcessor();

    DataNodeRegisterResp resp;
    DataNodeRegisterReq registerReq0 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6667));
    DataNodeRegisterReq registerReq1 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6668));
    DataNodeRegisterReq registerReq2 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6669));

    // test success register
    resp = processor.registerDataNode(registerReq0);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getRegisterResult().getCode());
    Assert.assertEquals(0, resp.getDataNodeID());
    resp = processor.registerDataNode(registerReq1);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getRegisterResult().getCode());
    Assert.assertEquals(1, resp.getDataNodeID());
    resp = processor.registerDataNode(registerReq2);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getRegisterResult().getCode());
    Assert.assertEquals(2, resp.getDataNodeID());

    // test query DataNodeInfo
    Map<Integer, DataNodeMessage> messageMap = processor.getDataNodesMessage(-1);
    Assert.assertEquals(3, messageMap.size());
    List<Map.Entry<Integer, DataNodeMessage>> messageList = new ArrayList<>(messageMap.entrySet());
    messageList.sort(Comparator.comparingInt(Map.Entry::getKey));
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(i, messageList.get(i).getValue().getDataNodeID());
      Assert.assertEquals("0.0.0.0", messageList.get(i).getValue().getEndPoint().getIp());
      Assert.assertEquals(6667 + i, messageList.get(i).getValue().getEndPoint().getPort());
    }

    messageMap = processor.getDataNodesMessage(1);
    Assert.assertEquals(1, messageMap.size());
    Assert.assertNotNull(messageMap.get(1));
    Assert.assertEquals("0.0.0.0", messageMap.get(1).getEndPoint().getIp());
    Assert.assertEquals(6668, messageMap.get(1).getEndPoint().getPort());
  }

  @Test
  public void setStorageGroupTest() throws TException, IOException {
    ConfigNodeRPCServerProcessor processor = new ConfigNodeRPCServerProcessor();

    TSStatus status;
    final String sg = "root.sg0";

    // failed because there are not enough DataNodes
    SetStorageGroupReq setReq = new SetStorageGroupReq(sg);
    status = processor.setStorageGroup(setReq);
    Assert.assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals("DataNode is not enough, please register more.", status.getMessage());

    // register DataNodes
    DataNodeRegisterReq registerReq0 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6667));
    DataNodeRegisterReq registerReq1 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6668));
    DataNodeRegisterReq registerReq2 = new DataNodeRegisterReq(new EndPoint("0.0.0.0", 6669));
    status = processor.registerDataNode(registerReq0).getRegisterResult();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.registerDataNode(registerReq1).getRegisterResult();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.registerDataNode(registerReq2).getRegisterResult();
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // set StorageGroup
    status = processor.setStorageGroup(setReq);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // query StorageGroupSchema
    Map<String, StorageGroupMessage> messageMap = processor.getStorageGroupsMessage();
    Assert.assertEquals(1, messageMap.size());
    Assert.assertNotNull(messageMap.get(sg));
    Assert.assertEquals(sg, messageMap.get(sg).getStorageGroup());
  }
}
