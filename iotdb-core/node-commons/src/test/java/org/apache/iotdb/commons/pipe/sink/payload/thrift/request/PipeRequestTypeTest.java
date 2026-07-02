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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipeRequestTypeTest {

  @Test
  public void testAllRequestTypesAreRecognized() {
    final Set<PipeRequestType> coveredRequestTypes = new HashSet<>();
    final Set<Short> usedWireTypes = new HashSet<>();

    for (final RequestTypeCase requestTypeCase : requestTypeCases()) {
      Assert.assertEquals(requestTypeCase.wireType, requestTypeCase.requestType.getType());
      Assert.assertTrue(
          "duplicated request type case: " + requestTypeCase.requestType,
          coveredRequestTypes.add(requestTypeCase.requestType));
      Assert.assertTrue(
          "duplicated wire type: " + requestTypeCase.wireType,
          usedWireTypes.add(requestTypeCase.wireType));
      Assert.assertTrue(PipeRequestType.isValidatedRequestType(requestTypeCase.wireType));
      Assert.assertEquals(
          requestTypeCase.requestType, PipeRequestType.valueOf(requestTypeCase.wireType));
    }

    Assert.assertEquals(PipeRequestType.values().length, coveredRequestTypes.size());
  }

  @Test
  public void testInvalidRequestTypesAreRejected() {
    for (final short invalidType :
        new short[] {
          Short.MIN_VALUE, -1, 14, 49, 52, 99, 103, 199, 203, 299, 301, 399, 401, Short.MAX_VALUE
        }) {
      Assert.assertFalse(PipeRequestType.isValidatedRequestType(invalidType));
      Assert.assertNull(PipeRequestType.valueOf(invalidType));
    }
  }

  private static List<RequestTypeCase> requestTypeCases() {
    return Arrays.asList(
        new RequestTypeCase((short) 0, PipeRequestType.HANDSHAKE_CONFIGNODE_V1),
        new RequestTypeCase((short) 1, PipeRequestType.HANDSHAKE_DATANODE_V1),
        new RequestTypeCase((short) 50, PipeRequestType.HANDSHAKE_CONFIGNODE_V2),
        new RequestTypeCase((short) 51, PipeRequestType.HANDSHAKE_DATANODE_V2),
        new RequestTypeCase((short) 2, PipeRequestType.TRANSFER_TABLET_INSERT_NODE),
        new RequestTypeCase((short) 3, PipeRequestType.TRANSFER_TABLET_RAW),
        new RequestTypeCase((short) 4, PipeRequestType.TRANSFER_TS_FILE_PIECE),
        new RequestTypeCase((short) 5, PipeRequestType.TRANSFER_TS_FILE_SEAL),
        new RequestTypeCase((short) 6, PipeRequestType.TRANSFER_TABLET_BATCH),
        new RequestTypeCase((short) 7, PipeRequestType.TRANSFER_TABLET_BINARY),
        new RequestTypeCase((short) 8, PipeRequestType.TRANSFER_TS_FILE_PIECE_WITH_MOD),
        new RequestTypeCase((short) 9, PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD),
        new RequestTypeCase((short) 10, PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2),
        new RequestTypeCase((short) 11, PipeRequestType.TRANSFER_TABLET_RAW_V2),
        new RequestTypeCase((short) 12, PipeRequestType.TRANSFER_TABLET_BINARY_V2),
        new RequestTypeCase((short) 13, PipeRequestType.TRANSFER_TABLET_BATCH_V2),
        // 1.3 named this request TRANSFER_SCHEMA_PLAN. 2.0 keeps the same wire type.
        new RequestTypeCase((short) 100, PipeRequestType.TRANSFER_PLAN_NODE),
        new RequestTypeCase((short) 101, PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE),
        new RequestTypeCase((short) 102, PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL),
        new RequestTypeCase((short) 200, PipeRequestType.TRANSFER_CONFIG_PLAN),
        new RequestTypeCase((short) 201, PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE),
        new RequestTypeCase((short) 202, PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL),
        new RequestTypeCase((short) 300, PipeRequestType.TRANSFER_COMPRESSED),
        new RequestTypeCase((short) 400, PipeRequestType.TRANSFER_SLICE));
  }

  private static class RequestTypeCase {

    private final short wireType;
    private final PipeRequestType requestType;

    private RequestTypeCase(final short wireType, final PipeRequestType requestType) {
      this.wireType = wireType;
      this.requestType = requestType;
    }
  }
}
