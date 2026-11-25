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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.pipe.sink.payload.thrift.common;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

public class PipeTransferIntegrityUtilsTest {

  @Test
  public void testCrc32IsOptionalAndDetectsRequestChanges() {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = 1;
    req.type = 100;
    req.body = ByteBuffer.wrap(new byte[] {1, 2, 3});

    Assert.assertFalse(req.isSetCrc32());
    PipeTransferIntegrityUtils.setCrc32(req);
    final long crc32 = req.getCrc32();

    Assert.assertEquals(PipeTransferIntegrityUtils.calculateCrc32(req), crc32);
    req.setBody(new byte[] {1, 2, 4});
    Assert.assertNotEquals(crc32, PipeTransferIntegrityUtils.calculateCrc32(req));
  }

  @Test
  public void testSha256FailureUsesCallerProvidedFallback() {
    final File missingFile = new File("missing-" + UUID.randomUUID() + ".tsfile");

    Assert.assertEquals(
        "unknown", PipeTransferIntegrityUtils.calculateSha256OrUnavailable(missingFile, "unknown"));
  }
}
