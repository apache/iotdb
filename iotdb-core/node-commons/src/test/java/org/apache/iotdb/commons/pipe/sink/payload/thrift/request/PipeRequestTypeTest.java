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

public class PipeRequestTypeTest {

  @Test
  public void testAllV13RequestTypesAreRecognized() {
    assertV13RequestType((short) 0, PipeRequestType.HANDSHAKE_CONFIGNODE_V1);
    assertV13RequestType((short) 1, PipeRequestType.HANDSHAKE_DATANODE_V1);
    assertV13RequestType((short) 50, PipeRequestType.HANDSHAKE_CONFIGNODE_V2);
    assertV13RequestType((short) 51, PipeRequestType.HANDSHAKE_DATANODE_V2);

    assertV13RequestType((short) 2, PipeRequestType.TRANSFER_TABLET_INSERT_NODE);
    assertV13RequestType((short) 3, PipeRequestType.TRANSFER_TABLET_RAW);
    assertV13RequestType((short) 4, PipeRequestType.TRANSFER_TS_FILE_PIECE);
    assertV13RequestType((short) 5, PipeRequestType.TRANSFER_TS_FILE_SEAL);
    assertV13RequestType((short) 6, PipeRequestType.TRANSFER_TABLET_BATCH);
    assertV13RequestType((short) 7, PipeRequestType.TRANSFER_TABLET_BINARY);
    assertV13RequestType((short) 8, PipeRequestType.TRANSFER_TS_FILE_PIECE_WITH_MOD);
    assertV13RequestType((short) 9, PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD);

    // 1.3 named this request TRANSFER_SCHEMA_PLAN. 2.0 keeps the same wire type.
    assertV13RequestType((short) 100, PipeRequestType.TRANSFER_PLAN_NODE);
    assertV13RequestType((short) 101, PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE);
    assertV13RequestType((short) 102, PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL);

    assertV13RequestType((short) 200, PipeRequestType.TRANSFER_CONFIG_PLAN);
    assertV13RequestType((short) 201, PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE);
    assertV13RequestType((short) 202, PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL);

    assertV13RequestType((short) 300, PipeRequestType.TRANSFER_COMPRESSED);
    assertV13RequestType((short) 400, PipeRequestType.TRANSFER_SLICE);
  }

  private static void assertV13RequestType(
      final short type, final PipeRequestType expectedRequestType) {
    Assert.assertTrue(PipeRequestType.isValidatedRequestType(type));
    Assert.assertEquals(expectedRequestType, PipeRequestType.valueOf(type));
  }
}
