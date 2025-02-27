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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeGuardian;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeReceiver;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSelector;
import org.apache.iotdb.consensus.pipe.consensuspipe.ProgressIndexManager;

import java.util.concurrent.TimeUnit;

public class PipeConsensusConfig {
  private final RPC rpc;
  private final Pipe pipe;
  private final ReplicateMode replicateMode;

  public PipeConsensusConfig(RPC rpc, Pipe pipe, ReplicateMode replicateMode) {
    this.rpc = rpc;
    this.pipe = pipe;
    this.replicateMode = replicateMode;
  }

  public ReplicateMode getReplicateMode() {
    return replicateMode;
  }

  public RPC getRpc() {
    return rpc;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private RPC rpc;
    private Pipe pipe;
    private ReplicateMode replicateMode;

    public Builder setPipe(Pipe pipe) {
      this.pipe = pipe;
      return this;
    }

    public Builder setRPC(RPC rpc) {
      this.rpc = rpc;
      return this;
    }

    public Builder setReplicateMode(ReplicateMode replicateMode) {
      this.replicateMode = replicateMode;
      return this;
    }

    public PipeConsensusConfig build() {
      return new PipeConsensusConfig(rpc, pipe, replicateMode);
    }
  }

  public static class RPC {
    private final int rpcSelectorThreadNum;
    private final int rpcMinConcurrentClientNum;
    private final int rpcMaxConcurrentClientNum;
    private final int thriftServerAwaitTimeForStopService;
    private final boolean isRpcThriftCompressionEnabled;
    private final int connectionTimeoutInMs;
    private final int thriftMaxFrameSize;

    public RPC(
        int rpcSelectorThreadNum,
        int rpcMinConcurrentClientNum,
        int rpcMaxConcurrentClientNum,
        int thriftServerAwaitTimeForStopService,
        boolean isRpcThriftCompressionEnabled,
        int connectionTimeoutInMs,
        int thriftMaxFrameSize) {
      this.rpcSelectorThreadNum = rpcSelectorThreadNum;
      this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
      this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
      this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
      this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
    }

    public int getRpcSelectorThreadNum() {
      return rpcSelectorThreadNum;
    }

    public int getRpcMinConcurrentClientNum() {
      return rpcMinConcurrentClientNum;
    }

    public int getRpcMaxConcurrentClientNum() {
      return rpcMaxConcurrentClientNum;
    }

    public int getThriftServerAwaitTimeForStopService() {
      return thriftServerAwaitTimeForStopService;
    }

    public boolean isRpcThriftCompressionEnabled() {
      return isRpcThriftCompressionEnabled;
    }

    public int getConnectionTimeoutInMs() {
      return connectionTimeoutInMs;
    }

    public int getThriftMaxFrameSize() {
      return thriftMaxFrameSize;
    }

    public static RPC.Builder newBuilder() {
      return new RPC.Builder();
    }

    public static class Builder {
      private int rpcSelectorThreadNum = 1;
      private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();
      private int rpcMaxConcurrentClientNum = 65535;
      private int thriftServerAwaitTimeForStopService = 60;
      private boolean isRpcThriftCompressionEnabled = false;
      private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(60);
      private int thriftMaxFrameSize = 536870912;

      public RPC.Builder setRpcSelectorThreadNum(int rpcSelectorThreadNum) {
        this.rpcSelectorThreadNum = rpcSelectorThreadNum;
        return this;
      }

      public RPC.Builder setRpcMinConcurrentClientNum(int rpcMinConcurrentClientNum) {
        this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
        return this;
      }

      public RPC.Builder setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
        this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
        return this;
      }

      public RPC.Builder setThriftServerAwaitTimeForStopService(
          int thriftServerAwaitTimeForStopService) {
        this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
        return this;
      }

      public RPC.Builder setIsRpcThriftCompressionEnabled(boolean isRpcThriftCompressionEnabled) {
        this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
        return this;
      }

      public RPC.Builder setConnectionTimeoutInMs(int connectionTimeoutInMs) {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        return this;
      }

      public RPC.Builder setThriftMaxFrameSize(int thriftMaxFrameSize) {
        this.thriftMaxFrameSize = thriftMaxFrameSize;
        return this;
      }

      public RPC build() {
        return new RPC(
            rpcSelectorThreadNum,
            rpcMinConcurrentClientNum,
            rpcMaxConcurrentClientNum,
            thriftServerAwaitTimeForStopService,
            isRpcThriftCompressionEnabled,
            connectionTimeoutInMs,
            thriftMaxFrameSize);
      }
    }
  }

  public static class Pipe {
    private final String extractorPluginName;
    private final String processorPluginName;
    private final String connectorPluginName;
    private final ConsensusPipeDispatcher consensusPipeDispatcher;
    private final ConsensusPipeGuardian consensusPipeGuardian;
    private final ConsensusPipeSelector consensusPipeSelector;
    private final ProgressIndexManager progressIndexManager;
    private final ConsensusPipeReceiver consensusPipeReceiver;
    private final long consensusPipeGuardJobIntervalInSeconds;

    public Pipe(
        String extractorPluginName,
        String processorPluginName,
        String connectorPluginName,
        ConsensusPipeDispatcher consensusPipeDispatcher,
        ConsensusPipeGuardian consensusPipeGuardian,
        ConsensusPipeSelector consensusPipeSelector,
        ProgressIndexManager progressIndexManager,
        ConsensusPipeReceiver consensusPipeReceiver,
        long consensusPipeGuardJobIntervalInSeconds) {
      this.extractorPluginName = extractorPluginName;
      this.processorPluginName = processorPluginName;
      this.connectorPluginName = connectorPluginName;
      this.consensusPipeDispatcher = consensusPipeDispatcher;
      this.consensusPipeGuardian = consensusPipeGuardian;
      this.consensusPipeSelector = consensusPipeSelector;
      this.progressIndexManager = progressIndexManager;
      this.consensusPipeReceiver = consensusPipeReceiver;
      this.consensusPipeGuardJobIntervalInSeconds = consensusPipeGuardJobIntervalInSeconds;
    }

    public String getExtractorPluginName() {
      return extractorPluginName;
    }

    public String getProcessorPluginName() {
      return processorPluginName;
    }

    public String getConnectorPluginName() {
      return connectorPluginName;
    }

    public ConsensusPipeDispatcher getConsensusPipeDispatcher() {
      return consensusPipeDispatcher;
    }

    public ConsensusPipeGuardian getConsensusPipeGuardian() {
      return consensusPipeGuardian;
    }

    public ConsensusPipeSelector getConsensusPipeSelector() {
      return consensusPipeSelector;
    }

    public ConsensusPipeReceiver getConsensusPipeReceiver() {
      return consensusPipeReceiver;
    }

    public ProgressIndexManager getProgressIndexManager() {
      return progressIndexManager;
    }

    public long getConsensusPipeGuardJobIntervalInSeconds() {
      return consensusPipeGuardJobIntervalInSeconds;
    }

    public static Pipe.Builder newBuilder() {
      return new Pipe.Builder();
    }

    public static class Builder {
      private String extractorPluginName = BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName();
      private String processorPluginName =
          BuiltinPipePlugin.PIPE_CONSENSUS_PROCESSOR.getPipePluginName();
      private String connectorPluginName =
          BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName();
      private ConsensusPipeDispatcher consensusPipeDispatcher = null;
      private ConsensusPipeGuardian consensusPipeGuardian = null;
      private ConsensusPipeSelector consensusPipeSelector = null;
      private ProgressIndexManager progressIndexManager = null;
      private ConsensusPipeReceiver consensusPipeReceiver = null;
      private long consensusPipeGuardJobIntervalInSeconds = 180L;

      public Pipe.Builder setExtractorPluginName(String extractorPluginName) {
        this.extractorPluginName = extractorPluginName;
        return this;
      }

      public Pipe.Builder setProcessorPluginName(String processorPluginName) {
        this.processorPluginName = processorPluginName;
        return this;
      }

      public Pipe.Builder setConnectorPluginName(String connectorPluginName) {
        this.connectorPluginName = connectorPluginName;
        return this;
      }

      public Pipe.Builder setConsensusPipeDispatcher(
          ConsensusPipeDispatcher consensusPipeDispatcher) {
        this.consensusPipeDispatcher = consensusPipeDispatcher;
        return this;
      }

      public Pipe.Builder setConsensusPipeGuardian(ConsensusPipeGuardian consensusPipeGuardian) {
        this.consensusPipeGuardian = consensusPipeGuardian;
        return this;
      }

      public Pipe.Builder setConsensusPipeSelector(ConsensusPipeSelector consensusPipeSelector) {
        this.consensusPipeSelector = consensusPipeSelector;
        return this;
      }

      public Pipe.Builder setConsensusPipeReceiver(ConsensusPipeReceiver consensusPipeReceiver) {
        this.consensusPipeReceiver = consensusPipeReceiver;
        return this;
      }

      public Pipe.Builder setProgressIndexManager(ProgressIndexManager progressIndexManager) {
        this.progressIndexManager = progressIndexManager;
        return this;
      }

      public Pipe.Builder setConsensusPipeGuardJobIntervalInSeconds(
          long consensusPipeGuardJobIntervalInSeconds) {
        this.consensusPipeGuardJobIntervalInSeconds = consensusPipeGuardJobIntervalInSeconds;
        return this;
      }

      public Pipe build() {
        return new Pipe(
            extractorPluginName,
            processorPluginName,
            connectorPluginName,
            consensusPipeDispatcher,
            consensusPipeGuardian,
            consensusPipeSelector,
            progressIndexManager,
            consensusPipeReceiver,
            consensusPipeGuardJobIntervalInSeconds);
      }
    }
  }

  public enum ReplicateMode {
    STREAM("stream"),
    BATCH("batch");

    private final String value;

    ReplicateMode(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static ReplicateMode fromValue(String value) {
      if (value.equalsIgnoreCase(STREAM.getValue())) {
        return STREAM;
      } else if (value.equalsIgnoreCase(BATCH.getValue())) {
        return BATCH;
      }
      // return batch by default
      return BATCH;
    }
  }
}
