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

import org.apache.iotdb.commons.utils.RegionMigrationRateLimiter;

import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.util.Iterator;

class RateLimitedGrpcLogAppender extends GrpcLogAppender {

  private final RegionMigrationRateLimiter rateLimiter = RegionMigrationRateLimiter.getInstance();

  RateLimitedGrpcLogAppender(
      RaftServer.Division server, LeaderState leaderState, FollowerInfo follower) {
    super(server, leaderState, follower);
  }

  @Override
  public Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(
      String requestId, SnapshotInfo snapshot) {
    final Iterable<InstallSnapshotRequestProto> requests =
        super.newInstallSnapshotRequests(requestId, snapshot);
    return () -> {
      final Iterator<InstallSnapshotRequestProto> iterator = requests.iterator();
      return new Iterator<InstallSnapshotRequestProto>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public InstallSnapshotRequestProto next() {
          final InstallSnapshotRequestProto request = iterator.next();
          rateLimiter.acquire(getSnapshotChunkDataSize(request));
          return request;
        }
      };
    };
  }

  static long getSnapshotChunkDataSize(InstallSnapshotRequestProto request) {
    if (!request.hasSnapshotChunk()) {
      return 0;
    }

    return request.getSnapshotChunk().getFileChunksList().stream()
        .map(FileChunkProto::getData)
        .mapToLong(data -> data == null ? 0 : data.size())
        .sum();
  }
}
