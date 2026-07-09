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
package org.apache.iotdb.consensus.ratis;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class RateLimitedGrpcLogAppenderTest {

  @Test
  public void testGetSnapshotChunkDataSize() {
    final InstallSnapshotRequestProto request =
        InstallSnapshotRequestProto.newBuilder()
            .setSnapshotChunk(
                InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder()
                    .addFileChunks(
                        FileChunkProto.newBuilder()
                            .setData(ByteString.copyFrom(new byte[3]))
                            .build())
                    .addFileChunks(
                        FileChunkProto.newBuilder()
                            .setData(ByteString.copyFrom(new byte[5]))
                            .build()))
            .build();

    Assert.assertEquals(8, RateLimitedGrpcLogAppender.getSnapshotChunkDataSize(request));
    Assert.assertEquals(
        0,
        RateLimitedGrpcLogAppender.getSnapshotChunkDataSize(
            InstallSnapshotRequestProto.newBuilder().buildPartial()));
  }

  @Test
  public void testRateLimitedGrpcRpcTypeIsResolvedByRatis() {
    final RaftProperties properties = new RaftProperties();

    RaftConfigKeys.Rpc.setType(properties, new RateLimitedGrpcRpcType());
    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties, ignored -> {});

    Assert.assertTrue(rpcType instanceof RateLimitedGrpcRpcType);
    Assert.assertTrue(rpcType.newFactory(new Parameters()) instanceof RateLimitedGrpcFactory);
  }
}
