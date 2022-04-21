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

package org.apache.iotdb.procedure.conf;

import java.io.File;

public class ProcedureNodeConfig {

  private String rpcAddress = "0.0.0.0";
  private int rpcPort = 22281;
  private int confignodePort = 22277;
  private int datanodePort = 22278;
  private int rpcMaxConcurrentClientNum = 65535;
  private boolean rpcAdvancedCompressionEnable = false;
  private boolean isRpcThriftCompressionEnabled = false;
  private int thriftMaxFrameSize = 536870912;
  private int thriftDefaultBufferSize = 1024;
  private int thriftServerAwaitTimeForStopService = 60;
  private String procedureWalDir =
      ProcedureNodeConstant.PROC_DIR + File.separator + ProcedureNodeConstant.WAL_DIR;
  private int completedEvictTTL = 800;
  private int completedCleanInterval = 30;
  private int workerThreadsCoreSize = Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  public String getRpcAddress() {
    return rpcAddress;
  }

  public void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getConfignodePort() {
    return confignodePort;
  }

  public void setConfignodePort(int confignodePort) {
    this.confignodePort = confignodePort;
  }

  public int getDatanodePort() {
    return datanodePort;
  }

  public void setDatanodePort(int datanodePort) {
    this.datanodePort = datanodePort;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
  }

  public String getProcedureWalDir() {
    return procedureWalDir;
  }

  public void setProcedureWalDir(String procedureWalDir) {
    this.procedureWalDir = procedureWalDir;
  }

  public int getCompletedEvictTTL() {
    return completedEvictTTL;
  }

  public void setCompletedEvictTTL(int completedEvictTTL) {
    this.completedEvictTTL = completedEvictTTL;
  }

  public int getCompletedCleanInterval() {
    return completedCleanInterval;
  }

  public void setCompletedCleanInterval(int completedCleanInterval) {
    this.completedCleanInterval = completedCleanInterval;
  }

  public boolean isRpcAdvancedCompressionEnable() {
    return rpcAdvancedCompressionEnable;
  }

  public void setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    this.rpcAdvancedCompressionEnable = rpcAdvancedCompressionEnable;
  }

  public int getWorkerThreadsCoreSize() {
    return workerThreadsCoreSize;
  }

  public void setWorkerThreadsCoreSize(int workerThreadsCoreSize) {
    this.workerThreadsCoreSize = workerThreadsCoreSize;
  }
}
