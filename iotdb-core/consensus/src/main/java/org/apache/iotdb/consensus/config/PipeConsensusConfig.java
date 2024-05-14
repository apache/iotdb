package org.apache.iotdb.consensus.config;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeGuardian;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSelector;
import org.apache.iotdb.consensus.pipe.consensuspipe.ProgressIndexManager;

public class PipeConsensusConfig {
  private final Pipe pipe;

  public PipeConsensusConfig(Pipe pipe) {
    this.pipe = pipe;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Pipe pipe;

    public Builder setPipe(Pipe pipe) {
      this.pipe = pipe;
      return this;
    }

    public PipeConsensusConfig build() {
      return new PipeConsensusConfig(pipe);
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
    private final long consensusPipeGuardJobIntervalInSeconds;

    public Pipe(
        String extractorPluginName,
        String processorPluginName,
        String connectorPluginName,
        ConsensusPipeDispatcher consensusPipeDispatcher,
        ConsensusPipeGuardian consensusPipeGuardian,
        ConsensusPipeSelector consensusPipeSelector,
        ProgressIndexManager progressIndexManager,
        long consensusPipeGuardJobIntervalInSeconds) {
      this.extractorPluginName = extractorPluginName;
      this.processorPluginName = processorPluginName;
      this.connectorPluginName = connectorPluginName;
      this.consensusPipeDispatcher = consensusPipeDispatcher;
      this.consensusPipeGuardian = consensusPipeGuardian;
      this.consensusPipeSelector = consensusPipeSelector;
      this.progressIndexManager = progressIndexManager;
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
          BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName();
      private String connectorPluginName =
          BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName();
      private ConsensusPipeDispatcher consensusPipeDispatcher = null;
      private ConsensusPipeGuardian consensusPipeGuardian = null;
      private ConsensusPipeSelector consensusPipeSelector = null;
      private ProgressIndexManager progressIndexManager = null;
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
            consensusPipeGuardJobIntervalInSeconds);
      }
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

import org.apache.iotdb.commons.client.property.ClientPoolProperty;

import java.util.concurrent.TimeUnit;

public class PipeConsensusConfig {


  public static class PipeConsensusRPCConfig {

    private int rpcSelectorThreadNum = 1;
    private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();
    private int rpcMaxConcurrentClientNum = 65535;
    private int thriftServerAwaitTimeForStopService = 60;
    private boolean isRpcThriftCompressionEnabled = false;
    private int selectorNumOfClientManager = 1;
    private int connectionTimeoutInMs = (int) TimeUnit.SECONDS.toMillis(20);
    private boolean printLogWhenThriftClientEncounterException = true;
    private int thriftMaxFrameSize = 536870912;
    private int maxClientNumForEachNode =
        ClientPoolProperty.DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

    public int getRpcMaxConcurrentClientNum() {
      return rpcMaxConcurrentClientNum;
    }

    public void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
      this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
    }

    public int getThriftServerAwaitTimeForStopService() {
      return thriftServerAwaitTimeForStopService;
    }

    public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
      this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
    }

    public boolean isRpcThriftCompressionEnabled() {
      return isRpcThriftCompressionEnabled;
    }

    public void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
      isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
    }

    public int getSelectorNumOfClientManager() {
      return selectorNumOfClientManager;
    }

    public void setSelectorNumOfClientManager(int selectorNumOfClientManager) {
      this.selectorNumOfClientManager = selectorNumOfClientManager;
    }

    public int getConnectionTimeoutInMs() {
      return connectionTimeoutInMs;
    }

    public void setConnectionTimeoutInMs(int connectionTimeoutInMs) {
      this.connectionTimeoutInMs = connectionTimeoutInMs;
    }

    public boolean isPrintLogWhenThriftClientEncounterException() {
      return printLogWhenThriftClientEncounterException;
    }

    public void setPrintLogWhenThriftClientEncounterException(
        boolean printLogWhenThriftClientEncounterException) {
      this.printLogWhenThriftClientEncounterException = printLogWhenThriftClientEncounterException;
    }

    public int getThriftMaxFrameSize() {
      return thriftMaxFrameSize;
    }

    public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
    }

    public int getMaxClientNumForEachNode() {
      return maxClientNumForEachNode;
    }

    public void setMaxClientNumForEachNode(int maxClientNumForEachNode) {
      this.maxClientNumForEachNode = maxClientNumForEachNode;
    }

    public int getRpcSelectorThreadNum() {
      return rpcSelectorThreadNum;
    }

    public void setRpcSelectorThreadNum(int rpcSelectorThreadNum) {
      this.rpcSelectorThreadNum = rpcSelectorThreadNum;
    }

    public int getRpcMinConcurrentClientNum() {
      return rpcMinConcurrentClientNum;
    }

    public void setRpcMinConcurrentClientNum(int rpcMinConcurrentClientNum) {
      this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
    }
  }
}
