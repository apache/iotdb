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

package org.apache.iotdb.metrics.metricsets.net;

import oshi.SystemInfo;
import oshi.hardware.NetworkIF;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WindowsNetMetricManager implements INetMetricManager {
  private final SystemInfo systemInfo = new SystemInfo();
  private final int pid;

  public WindowsNetMetricManager() {
    this.pid = systemInfo.getOperatingSystem().getCurrentProcess().getProcessID();
  }

  @Override
  public Map<String, Long> getReceivedByte() {
    Map<String, Long> result = new HashMap<>();
    systemInfo
        .getHardware()
        .getNetworkIFs()
        .forEach(
            (networkIF) -> {
              result.put(networkIF.getDisplayName(), networkIF.getBytesRecv());
            });
    return result;
  }

  @Override
  public Map<String, Long> getTransmittedBytes() {
    Map<String, Long> result = new HashMap<>();
    systemInfo
        .getHardware()
        .getNetworkIFs()
        .forEach(
            (networkIF) -> {
              result.put(networkIF.getDisplayName(), networkIF.getBytesSent());
            });
    return result;
  }

  @Override
  public Map<String, Long> getReceivedPackets() {
    Map<String, Long> result = new HashMap<>();
    systemInfo
        .getHardware()
        .getNetworkIFs()
        .forEach(
            (networkIF) -> {
              result.put(networkIF.getDisplayName(), networkIF.getPacketsRecv());
            });
    return result;
  }

  @Override
  public Map<String, Long> getTransmittedPackets() {
    Map<String, Long> result = new HashMap<>();
    systemInfo
        .getHardware()
        .getNetworkIFs()
        .forEach(
            (networkIF) -> {
              result.put(networkIF.getDisplayName(), networkIF.getPacketsSent());
            });
    return result;
  }

  @Override
  public Set<String> getIfaceSet() {
    return systemInfo.getHardware().getNetworkIFs().stream()
        .map(NetworkIF::getDisplayName)
        .collect(Collectors.toSet());
  }

  @Override
  public int getConnectionNum() {
    return (int)
        systemInfo.getOperatingSystem().getInternetProtocolStats().getConnections().stream()
            .filter(conn -> conn.getowningProcessId() == pid)
            .count();
  }
}
