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

import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WindowsNetMetricManager implements INetMetricManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WindowsNetMetricManager.class);

  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  private long lastUpdateTime = 0L;

  private Set<String> ifaceSet = new HashSet<>();

  private final Map<String, Long> receivedBytesMapForIface = new HashMap<>();

  private final Map<String, Long> transmittedBytesMapForIface = new HashMap<>();

  private final Map<String, Long> receivedPacketsMapForIface = new HashMap<>();

  private final Map<String, Long> transmittedPacketsMapForIface = new HashMap<>();

  private int connectionNum = 0;

  public WindowsNetMetricManager() {}

  @Override
  public Set<String> getIfaceSet() {
    checkUpdate();
    return ifaceSet;
  }

  @Override
  public Map<String, Long> getReceivedByte() {
    checkUpdate();
    return receivedBytesMapForIface;
  }

  @Override
  public Map<String, Long> getTransmittedBytes() {
    checkUpdate();
    return transmittedBytesMapForIface;
  }

  @Override
  public Map<String, Long> getReceivedPackets() {
    checkUpdate();
    return receivedPacketsMapForIface;
  }

  @Override
  public Map<String, Long> getTransmittedPackets() {
    checkUpdate();
    return transmittedPacketsMapForIface;
  }

  @Override
  public int getConnectionNum() {
    checkUpdate();
    return connectionNum;
  }

  private void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime >= MetricConstant.UPDATE_INTERVAL) {
      updateNetStatus();
    }
  }

  private void updateNetStatus() {
    lastUpdateTime = System.currentTimeMillis();
    if (ifaceSet.isEmpty()) {
      updateInterfaces();
    }
    updateStatistics();
    if (MetricLevel.higherOrEqual(MetricLevel.NORMAL, METRIC_CONFIG.getMetricLevel())) {
      updateConnectionNum();
    }
  }

  private void updateInterfaces() {
    try {
      ifaceSet.clear();
      Process process =
          Runtime.getRuntime()
              .exec(
                  "cmd.exe /c chcp 65001 > nul & powershell.exe -Command \"Get-NetAdapter -IncludeHidden | Select Name | Format-List \"");
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("Name :")) {
          ifaceSet.add(line.substring("Name : ".length()).trim());
        }
      }
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        LOGGER.error("Failed to get interfaces, exit code: {}", exitCode);
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.error("Error updating interfaces", e);
      ifaceSet.clear();
    }
  }

  private void updateStatistics() {
    try {
      receivedBytesMapForIface.clear();
      transmittedBytesMapForIface.clear();
      receivedPacketsMapForIface.clear();
      transmittedPacketsMapForIface.clear();
      Process process =
          Runtime.getRuntime()
              .exec(
                  "cmd.exe /c chcp 65001 > nul & powershell.exe -Command \"Get-NetAdapterStatistics -IncludeHidden | Format-List Name,ReceivedBytes,SentBytes,ReceivedUnicastPackets,SentUnicastPackets \"");
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
      String line;
      String currentName = null;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("Name ")) {
          currentName = line.substring(line.indexOf(": ") + 2).trim();
        } else if (line.startsWith("ReceivedBytes ") && currentName != null) {
          long value = Long.parseLong(line.substring(line.indexOf(": ") + 2).trim());
          receivedBytesMapForIface.put(currentName, value);
        } else if (line.startsWith("SentBytes ") && currentName != null) {
          long value = Long.parseLong(line.substring(line.indexOf(": ") + 2).trim());
          transmittedBytesMapForIface.put(currentName, value);
        } else if (line.startsWith("ReceivedUnicastPackets ") && currentName != null) {
          long value = Long.parseLong(line.substring(line.indexOf(": ") + 2).trim());
          receivedPacketsMapForIface.put(currentName, value);
        } else if (line.startsWith("SentUnicastPackets ") && currentName != null) {
          long value = Long.parseLong(line.substring(line.indexOf(": ") + 2).trim());
          transmittedPacketsMapForIface.put(currentName, value);
          currentName = null; // Reset after processing an interface
        }
      }
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        LOGGER.error("Failed to get statistics, exit code: {}", exitCode);
      }
    } catch (IOException | InterruptedException | NumberFormatException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.error("Error updating statistics", e);
    }
  }

  private void updateConnectionNum() {
    try {
      Process process = Runtime.getRuntime().exec("netstat -ano");
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      int count = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("Active") && !line.startsWith("Proto")) {
          String[] parts = line.split("\\s+");
          if (parts.length >= 5 && parts[parts.length - 1].equals(METRIC_CONFIG.getPid())) {
            count++;
          }
        }
      }
      this.connectionNum = count;
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        LOGGER.error("Failed to get connection num, exit code: {}", exitCode);
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.error("Error updating connection num", e);
    }
  }
}
