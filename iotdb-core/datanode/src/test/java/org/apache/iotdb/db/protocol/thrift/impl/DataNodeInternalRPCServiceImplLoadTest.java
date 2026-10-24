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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataNodeInternalRPCServiceImplLoadTest {

  private int originalDataNodeId;

  @Before
  public void setUp() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testMissingSliceMetadataReportsEveryMissingField() {
    final DataNodeInternalRPCServiceImpl service =
        Mockito.mock(DataNodeInternalRPCServiceImpl.class, Mockito.CALLS_REAL_METHODS);
    for (int mask = 1; mask < 7; mask++) {
      final TTsFilePieceReq request =
          new TTsFilePieceReq(
              ByteBuffer.wrap(new byte[] {1}),
              "test-uuid",
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
      final List<String> missingFields = new ArrayList<>();
      if ((mask & 1) != 0) {
        request.setSliceIndex(0);
      } else {
        missingFields.add("sliceIndex");
      }
      if ((mask & 2) != 0) {
        request.setSliceCount(2);
      } else {
        missingFields.add("sliceCount");
      }
      if ((mask & 4) != 0) {
        request.setOriginBodySize(2);
      } else {
        missingFields.add("originBodySize");
      }
      final TLoadResp response = service.sendTsFilePieceNode(request);
      Assert.assertFalse(response.isAccepted());
      Assert.assertEquals(
          TSStatusCode.DESERIALIZE_PIECE_OF_TSFILE_ERROR.getStatusCode(),
          response.getStatus().getCode());
      Assert.assertEquals(
          String.format(
              DataNodeMiscMessages.MESSAGE_MISSING_LOAD_TSFILE_SLICE_METADATA_ARG_DE4333DA,
              String.join(", ", missingFields)),
          response.getStatus().getMessage());
      Assert.assertEquals(response.getStatus().getMessage(), response.getMessage());
    }
  }
}
