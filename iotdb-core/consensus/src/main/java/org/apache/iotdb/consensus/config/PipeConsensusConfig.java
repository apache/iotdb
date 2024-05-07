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
