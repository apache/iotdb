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

package org.apache.iotdb.consensus.config;

import java.util.Optional;

public class TRaftConfig {

  private final Replication replication;
  private final Election election;

  private TRaftConfig(Replication replication, Election election) {
    this.replication = replication;
    this.election = election;
  }

  public Replication getReplication() {
    return replication;
  }

  public Election getElection() {
    return election;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Replication replication;
    private Election election;

    public Builder setReplication(Replication replication) {
      this.replication = replication;
      return this;
    }

    public Builder setElection(Election election) {
      this.election = election;
      return this;
    }

    public TRaftConfig build() {
      return new TRaftConfig(
          Optional.ofNullable(replication).orElseGet(() -> Replication.newBuilder().build()),
          Optional.ofNullable(election).orElseGet(() -> Election.newBuilder().build()));
    }
  }

  public static class Replication {

    private final int maxPendingRetryEntriesPerFollower;
    private final long waitingReplicationTimeMs;

    private Replication(int maxPendingRetryEntriesPerFollower, long waitingReplicationTimeMs) {
      this.maxPendingRetryEntriesPerFollower = maxPendingRetryEntriesPerFollower;
      this.waitingReplicationTimeMs = waitingReplicationTimeMs;
    }

    public int getMaxPendingRetryEntriesPerFollower() {
      return maxPendingRetryEntriesPerFollower;
    }

    public long getWaitingReplicationTimeMs() {
      return waitingReplicationTimeMs;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private int maxPendingRetryEntriesPerFollower = 100_000;
      private long waitingReplicationTimeMs = 1L;

      public Builder setMaxPendingRetryEntriesPerFollower(int maxPendingRetryEntriesPerFollower) {
        this.maxPendingRetryEntriesPerFollower = maxPendingRetryEntriesPerFollower;
        return this;
      }

      public Builder setWaitingReplicationTimeMs(long waitingReplicationTimeMs) {
        this.waitingReplicationTimeMs = waitingReplicationTimeMs;
        return this;
      }

      public Replication build() {
        return new Replication(maxPendingRetryEntriesPerFollower, waitingReplicationTimeMs);
      }
    }
  }

  public static class Election {

    private final long randomSeed;

    private Election(long randomSeed) {
      this.randomSeed = randomSeed;
    }

    public long getRandomSeed() {
      return randomSeed;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private long randomSeed = System.nanoTime();

      public Builder setRandomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
        return this;
      }

      public Election build() {
        return new Election(randomSeed);
      }
    }
  }
}
