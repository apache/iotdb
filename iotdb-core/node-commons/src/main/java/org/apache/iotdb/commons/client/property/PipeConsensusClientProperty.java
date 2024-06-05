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

package org.apache.iotdb.commons.client.property;

/** This class defines the configurations used by the PipeConsensus Client. */
public class PipeConsensusClientProperty {
  private final boolean isRpcThriftCompressionEnabled;
  private final int selectorNumOfClientManager;
  private final boolean printLogWhenThriftClientEncounterException;
  private final int maxClientNumForEachNode;

  public PipeConsensusClientProperty(
      boolean isRpcThriftCompressionEnabled,
      int selectorNumOfClientManager,
      boolean printLogWhenThriftClientEncounterException,
      int maxClientNumForEachNode) {
    this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
    this.selectorNumOfClientManager = selectorNumOfClientManager;
    this.printLogWhenThriftClientEncounterException = printLogWhenThriftClientEncounterException;
    this.maxClientNumForEachNode = maxClientNumForEachNode;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public boolean isPrintLogWhenThriftClientEncounterException() {
    return printLogWhenThriftClientEncounterException;
  }

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public static PipeConsensusClientProperty.Builder newBuilder() {
    return new PipeConsensusClientProperty.Builder();
  }

  public static class Builder {
    private boolean isRpcThriftCompressionEnabled = false;
    private int selectorNumOfClientManager = 1;
    private boolean printLogWhenThriftClientEncounterException = true;
    private int maxClientNumForEachNode =
        ClientPoolProperty.DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

    public PipeConsensusClientProperty.Builder setIsRpcThriftCompressionEnabled(
        boolean isRpcThriftCompressionEnabled) {
      this.isRpcThriftCompressionEnabled = isRpcThriftCompressionEnabled;
      return this;
    }

    public PipeConsensusClientProperty.Builder setSelectorNumOfClientManager(
        int selectorNumOfClientManager) {
      this.selectorNumOfClientManager = selectorNumOfClientManager;
      return this;
    }

    public PipeConsensusClientProperty.Builder setPrintLogWhenThriftClientEncounterException(
        boolean printLogWhenThriftClientEncounterException) {
      this.printLogWhenThriftClientEncounterException = printLogWhenThriftClientEncounterException;
      return this;
    }

    public PipeConsensusClientProperty.Builder setMaxClientNumForEachNode(
        int maxClientNumForEachNode) {
      this.maxClientNumForEachNode = maxClientNumForEachNode;
      return this;
    }

    public PipeConsensusClientProperty build() {
      return new PipeConsensusClientProperty(
          isRpcThriftCompressionEnabled,
          selectorNumOfClientManager,
          printLogWhenThriftClientEncounterException,
          maxClientNumForEachNode);
    }
  }
}
