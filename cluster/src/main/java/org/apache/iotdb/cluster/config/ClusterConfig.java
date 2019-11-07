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

import java.util.Arrays;
import java.util.List;

public class ClusterConfig {

  static final String CONFIG_NAME = "iotdb-cluster.properties";

  private String localIP = "localhost";
  private int localMetaPort = 9003;

  // each one is {IP | domain name}:{meta port}
  private List<String> seedNodeUrls = Arrays.asList("localhost:9003", "localhost"
      + ":9004", "localhost:9005");

  private boolean isRpcThriftCompressionEnabled = true;
  private int maxConcurrentClientNum = 1024;


  public String getLocalIP() {
    return localIP;
  }

  void setLocalIP(String localIP) {
    this.localIP = localIP;
  }

  public int getLocalMetaPort() {
    return localMetaPort;
  }

  void setLocalMetaPort(int localMetaPort) {
    this.localMetaPort = localMetaPort;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  public int getMaxConcurrentClientNum() {
    return maxConcurrentClientNum;
  }

  void setMaxConcurrentClientNum(int maxConcurrentClientNum) {
    this.maxConcurrentClientNum = maxConcurrentClientNum;
  }

  public List<String> getSeedNodeUrls() {
    return seedNodeUrls;
  }

  void setSeedNodeUrls(List<String> seedNodeUrls) {
    this.seedNodeUrls = seedNodeUrls;
  }
}
