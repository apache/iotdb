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

package org.apache.iotdb.db.pipe.receiver.protocol.legacy;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.sink.payload.legacy.TsFilePipeData;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class IoTDBLegacyPipeReceiverAgentTest {

  private static final String PIPE_NAME = "poc";
  private static final long CREATE_TIME = 1700000000000L;
  private static final String REMOTE_ADDRESS = "127.0.0.1";

  private String originalSyncDir;
  private Path syncDir;
  private IoTDBLegacyPipeReceiverAgent agent;

  @Before
  public void setUp() throws Exception {
    originalSyncDir = CommonDescriptor.getInstance().getConfig().getSyncDir();
    syncDir = Files.createTempDirectory("legacy-pipe-receiver");
    CommonDescriptor.getInstance().getConfig().setSyncDir(syncDir.toString());

    agent = new IoTDBLegacyPipeReceiverAgent();
    final TSStatus status =
        agent.handshake(
            new TSyncIdentityInfo(PIPE_NAME, CREATE_TIME, "UNKNOWN", ""),
            REMOTE_ADDRESS,
            null,
            null);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @After
  public void tearDown() throws Exception {
    if (agent != null) {
      agent.handleClientExit();
    }
    CommonDescriptor.getInstance().getConfig().setSyncDir(originalSyncDir);
    if (syncDir != null) {
      org.apache.tsfile.external.commons.io.FileUtils.deleteDirectory(syncDir.toFile());
    }
  }

  @Test
  public void testTransportFileRejectsPathTraversal() throws Exception {
    final String traversal =
        ".." + File.separator + ".." + File.separator + ".." + File.separator + "pwned";

    final TSStatus status =
        agent.transportFile(
            new TSyncTransportMetaInfo(traversal, 0),
            ByteBuffer.wrap("pwned".getBytes(StandardCharsets.UTF_8)));

    Assert.assertEquals(TSStatusCode.SYNC_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("Illegal fileName"));
    Assert.assertFalse(Files.exists(syncDir.resolve("pwned.patch")));
  }

  @Test
  public void testTransportFileWritesPlainFileUnderFileDataDir() throws Exception {
    final String fileName = "1-2-3-4.tsfile";
    final byte[] payload = "iotdb".getBytes(StandardCharsets.UTF_8);

    final TSStatus status =
        agent.transportFile(new TSyncTransportMetaInfo(fileName, 0), ByteBuffer.wrap(payload));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    final Path patchFile = getFileDataDir().resolve(fileName + ".patch");
    Assert.assertArrayEquals(payload, Files.readAllBytes(patchFile));
  }

  @Test
  public void testTransportPipeDataRejectsPathTraversalTsFileName() throws Exception {
    final String traversal = ".." + File.separator + "evil.tsfile";

    final TSStatus status =
        agent.transportPipeData(ByteBuffer.wrap(new TsFilePipeData("", traversal, -1).serialize()));

    Assert.assertEquals(TSStatusCode.PIPESERVER_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("Illegal fileName"));
  }

  private Path getFileDataDir() {
    return syncDir
        .resolve("receiver")
        .resolve(String.format("%s-%d-%s", PIPE_NAME, CREATE_TIME, REMOTE_ADDRESS))
        .resolve("file-data");
  }
}
