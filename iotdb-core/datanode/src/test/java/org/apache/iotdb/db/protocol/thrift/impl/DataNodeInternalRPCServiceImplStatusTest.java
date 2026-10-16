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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataNodeInternalRPCServiceImplStatusTest {

  @BeforeClass
  public static void setUpDataNodeId() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @Test
  public void testDeleteRegionStatusCombinesConsensusAndLocalResults() {
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        DataNodeInternalRPCServiceImpl.getDeleteRegionStatus(
                new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode()), true)
            .getCode());
    Assert.assertEquals(
        TSStatusCode.REGION_NOT_EXIST.getStatusCode(),
        DataNodeInternalRPCServiceImpl.getDeleteRegionStatus(
                new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode()), false)
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        DataNodeInternalRPCServiceImpl.getDeleteRegionStatus(
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), false)
            .getCode());
    Assert.assertEquals(
        TSStatusCode.DELETE_REGION_ERROR.getStatusCode(),
        DataNodeInternalRPCServiceImpl.getDeleteRegionStatus(
                new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode()), true)
            .getCode());
    Assert.assertEquals(
        TSStatusCode.DELETE_REGION_ERROR.getStatusCode(),
        DataNodeInternalRPCServiceImpl.getDeleteRegionStatus(
                new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode()), false)
            .getCode());
  }
}
