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
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeReceiver;
import org.apache.iotdb.consensus.pipe.consensuspipe.ReplicateProgressManager;

import java.util.concurrent.TimeUnit;

public class IoTConsensusV2Config {
  private final RPC rpc;
  private final Pipe pipe;
  private final ReplicateMode replicateMode;

  public IoTConsensusV2Config(RPC rpc, Pipe pipe, ReplicateMode replicateMode) {
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

    public IoTConsensusV2Config build() {
      return new IoTConsensusV2Config(rpc, pipe, replicateMode);
    }
  }

  public static class RPC {
    private final int rpcMaxConcurrentClientNum;
    private final int thriftServerAwaitTimeForStopService;
    private final boolean isRpcThriftCompressionEnabled;
    private final int connectionTimeoutInMs;
    private final int thriftMaxFrameSize;

    private boolean isEnableSSL = false;
    private String sslTrustStorePath = "";
    private String sslTrustStorePassword = "";
    private String sslKeyStorePath = "";
    private String sslKeyStorePassword = "";

    public RPC(
        int rpcMaxConcurrentClientNum,
        int thriftServerAwaitTimeForStopService,
        boolean isRpcThriftCompressionEnabled,
        int connectionTimeoutInMs,
        int thriftMaxFrameSize,
        boolean isEnableSSL,
        String sslTrustStorePath,
        String sslTrustStorePassword,
        String sslKeyStorePath,
        String sslKeyStorePassword) {
      this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
      this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
      this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      this.isEnableSSL = isEnableSSL;
      this.sslTrustStorePath = sslTrustStorePath;
      this.sslTrustStorePassword = sslTrustStorePassword;
      this.sslKeyStorePath = sslKeyStorePath;
      this.sslKeyStorePassword = sslKeyStorePassword;
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

    public boolean isEnableSSL() {
      return isEnableSSL;
    }

    public String getSslTrustStorePath() {
      return sslTrustStorePath;
    }

    public String getSslTrustStorePassword() {
      return sslTrustStorePassword;
    }

    public String getSslKeyStorePath() {
      return sslKeyStorePath;
    }

    public String getSslKeyStorePassword() {
      return sslKeyStorePassword;
    }

    public static RPC.Builder newBuilder() {
      return new RPC.Builder();
    }

    public static class Builder {
      private int rpcMaxConcurrentClientNum = 65535;
      private int thriftServerAwaitTimeForStopService = 60;
      private boolean isRpcThriftCompressionEnabled = false;
      private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(60);
      private int thriftMaxFrameSize = 536870912;

      private boolean isEnableSSL = false;
      private String sslTrustStorePath = "";
      private String sslTrustStorePassword = "";
      private String sslKeyStorePath = "";
      private String sslKeyStorePassword = "";

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

      public Builder setEnableSSL(boolean isEnableSSL) {
        this.isEnableSSL = isEnableSSL;
        return this;
      }

      public Builder setSslTrustStorePath(String sslTrustStorePath) {
        this.sslTrustStorePath = sslTrustStorePath;
        return this;
      }

      public Builder setSslTrustStorePassword(String sslTrustStorePassword) {
        this.sslTrustStorePassword = sslTrustStorePassword;
        return this;
      }

      public Builder setSslKeyStorePath(String sslKeyStorePath) {
        this.sslKeyStorePath = sslKeyStorePath;
        return this;
      }

      public Builder setSslKeyStorePassword(String sslKeyStorePassword) {
        this.sslKeyStorePassword = sslKeyStorePassword;
        return this;
      }

      public RPC build() {
        return new RPC(
            rpcMaxConcurrentClientNum,
            thriftServerAwaitTimeForStopService,
            isRpcThriftCompressionEnabled,
            connectionTimeoutInMs,
            thriftMaxFrameSize,
            isEnableSSL,
            sslTrustStorePath,
            sslTrustStorePassword,
            sslKeyStorePath,
            sslKeyStorePassword);
      }
    }
  }

  public static class Pipe {
    private final String extractorPluginName;
    private final String processorPluginName;
    private final String connectorPluginName;
    private final ReplicateProgressManager replicateProgressManager;
    private final ConsensusPipeReceiver consensusPipeReceiver;

    public Pipe(
        String extractorPluginName,
        String processorPluginName,
        String connectorPluginName,
        ReplicateProgressManager replicateProgressManager,
        ConsensusPipeReceiver consensusPipeReceiver) {
      this.extractorPluginName = extractorPluginName;
      this.processorPluginName = processorPluginName;
      this.connectorPluginName = connectorPluginName;
      this.replicateProgressManager = replicateProgressManager;
      this.consensusPipeReceiver = consensusPipeReceiver;
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

    public ConsensusPipeReceiver getConsensusPipeReceiver() {
      return consensusPipeReceiver;
    }

    public ReplicateProgressManager getProgressIndexManager() {
      return replicateProgressManager;
    }

    public static Pipe.Builder newBuilder() {
      return new Pipe.Builder();
    }

    public static class Builder {
      private String extractorPluginName = BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName();
      private String processorPluginName =
          BuiltinPipePlugin.IOT_CONSENSUS_V2_PROCESSOR.getPipePluginName();
      private String connectorPluginName =
          BuiltinPipePlugin.IOT_CONSENSUS_V2_ASYNC_CONNECTOR.getPipePluginName();
      private ReplicateProgressManager replicateProgressManager = null;
      private ConsensusPipeReceiver consensusPipeReceiver = null;

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

      public Pipe.Builder setConsensusPipeReceiver(ConsensusPipeReceiver consensusPipeReceiver) {
        this.consensusPipeReceiver = consensusPipeReceiver;
        return this;
      }

      public Pipe.Builder setProgressIndexManager(
          ReplicateProgressManager replicateProgressManager) {
        this.replicateProgressManager = replicateProgressManager;
        return this;
      }

      public Pipe build() {
        return new Pipe(
            extractorPluginName,
            processorPluginName,
            connectorPluginName,
            replicateProgressManager,
            consensusPipeReceiver);
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
