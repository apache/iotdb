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
package org.apache.iotdb.cluster.config;

import java.util.Collections;
import java.util.List;

public class ClusterConfig {

  static final String CONFIG_NAME = "iotdb-cluster.properties";

  private String localIP = "127.0.0.1";
  private int localMetaPort = 9003;

  // each one is {IP | domain name}:{meta port}
  private List<String> seedNodeUrls = Collections.emptyList();

  private boolean isRpcThriftCompressionEnabled = true;
  private int maxConcurrentClientNum = 1024;


  public String getLocalIP() {
    return localIP;
  }

  public void setLocalIP(String localIP) {
    this.localIP = localIP;
  }

  public int getLocalMetaPort() {
    return localMetaPort;
  }

  public void setLocalMetaPort(int localMetaPort) {
    this.localMetaPort = localMetaPort;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  public int getMaxConcurrentClientNum() {
    return maxConcurrentClientNum;
  }

  public void setMaxConcurrentClientNum(int maxConcurrentClientNum) {
    this.maxConcurrentClientNum = maxConcurrentClientNum;
  }

  public List<String> getSeedNodeUrls() {
    return seedNodeUrls;
  }

  public void setSeedNodeUrls(List<String> seedNodeUrls) {
    this.seedNodeUrls = seedNodeUrls;
  }
}
