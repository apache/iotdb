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

import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodesInfo;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

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
    info = processor.getDataNodesInfo(Integer.MAX_VALUE);
    info = processor.getDataNodesInfo(Integer.MIN_VALUE);
  }
}
