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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

public class LinuxNetMetricManager implements INetMetricManager {
  private final Logger log = LoggerFactory.getLogger(LinuxNetMetricManager.class);

  @SuppressWarnings("squid:S1075")
  private static final String IFACE_ID_PATH = "/sys/class/net/";

  @SuppressWarnings("squid:S1075")
  private static final String NET_STATUS_PATH = "/proc/net/dev";

  private static final String BYTES = "bytes";
  private static final String PACKETS = "packets";
  private static final long UPDATE_INTERVAL = 10_000L;

  private static final int IFACE_NAME_INDEX = 0;
  // initialized after reading status file
  private int receivedBytesIndex = 0;
  private int transmittedBytesIndex = 0;
  private int receivedPacketsIndex = 0;
  private int transmittedPacketsIndex = 0;
  private Set<String> ifaceSet;

  private final Map<String, Long> receivedBytesMapForIface;
  private final Map<String, Long> transmittedBytesMapForIface;
  private final Map<String, Long> receivedPacketsMapForIface;
  private final Map<String, Long> transmittedPacketsMapForIface;

  public LinuxNetMetricManager() {
    collectIfaces();
    // leave one entry to avoid hashmap resizing
    receivedBytesMapForIface = new HashMap<>(ifaceSet.size() + 1, 1);
    transmittedBytesMapForIface = new HashMap<>(ifaceSet.size() + 1, 1);
    receivedPacketsMapForIface = new HashMap<>(ifaceSet.size() + 1, 1);
    transmittedPacketsMapForIface = new HashMap<>(ifaceSet.size() + 1, 1);
    collectNetStatusIndex();
  }

  private long lastUpdateTime = 0L;

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

  private void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime >= UPDATE_INTERVAL) {
      updateNetStatus();
    }
  }

  private void collectIfaces() {
    File ifaceIdFolder = new File(IFACE_ID_PATH);
    if (!ifaceIdFolder.exists()) {
      ifaceSet = Collections.emptySet();
      log.warn("Cannot find {}", IFACE_ID_PATH);
      return;
    }
    ifaceSet =
        new ArrayList<>(Arrays.asList(Objects.requireNonNull(ifaceIdFolder.listFiles())))
            .stream().map(File::getName).collect(Collectors.toSet());
  }

  private void collectNetStatusIndex() {
    File netStatusFile = new File(NET_STATUS_PATH);
    if (!netStatusFile.exists()) {
      log.warn("Cannot find {}", NET_STATUS_PATH);
      return;
    }
    try (FileInputStream inputStream = new FileInputStream(netStatusFile)) {
      Scanner scanner = new Scanner(inputStream);
      // skip the first line
      scanner.nextLine();
      String headerLine = scanner.nextLine();
      String[] seperatedHeaderLine = headerLine.split("\\|");
      String[] receiveStatusHeader = seperatedHeaderLine[1].split("\\s+");
      String[] transmitStatusHeader = seperatedHeaderLine[2].split("\\s+");
      for (int i = 0, length = receiveStatusHeader.length; i < length; ++i) {
        if (receiveStatusHeader[i].equals(BYTES)) {
          receivedBytesIndex = i + 1;
        } else if (receiveStatusHeader[i].equals(PACKETS)) {
          receivedPacketsIndex = i + 1;
        }
      }
      for (int i = 0, length = transmitStatusHeader.length; i < length; ++i) {
        if (transmitStatusHeader[i].equals(BYTES)) {
          transmittedBytesIndex = i + length + 1;
        } else if (transmitStatusHeader[i].equals(PACKETS)) {
          transmittedPacketsIndex = i + length + 1;
        }
      }
    } catch (IOException e) {
      log.error("Meets exception when reading {}", NET_STATUS_PATH, e);
    }
  }

  private void updateNetStatus() {
    lastUpdateTime = System.currentTimeMillis();
    File netStatusFile = new File(NET_STATUS_PATH);
    if (!netStatusFile.exists()) {
      return;
    }
    try (FileInputStream inputStream = new FileInputStream(netStatusFile)) {
      Scanner scanner = new Scanner(inputStream);
      // skip the starting two lines
      // because they are the meta info
      scanner.nextLine();
      scanner.nextLine();

      // reading the actual status info for iface
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        // we wrap the string array as array list to execute the removing step
        List<String> statusInfoAsList = new ArrayList<>(Arrays.asList(line.split("\\s")));
        // remove all empty string
        statusInfoAsList.removeIf(x -> x.equals(""));

        String iface = statusInfoAsList.get(IFACE_NAME_INDEX);
        // since the read iface format is "IFACE:"
        // we need to remove the last letter
        iface = iface.substring(0, iface.length() - 1);

        long receivedBytes = Long.parseLong(statusInfoAsList.get(receivedBytesIndex));
        receivedBytesMapForIface.put(iface, receivedBytes);
        long transmittedBytes = Long.parseLong(statusInfoAsList.get(transmittedBytesIndex));
        transmittedBytesMapForIface.put(iface, transmittedBytes);
        long receivedPackets = Long.parseLong(statusInfoAsList.get(receivedPacketsIndex));
        receivedPacketsMapForIface.put(iface, receivedPackets);
        long transmittedPackets = Long.parseLong(statusInfoAsList.get(transmittedPacketsIndex));
        transmittedPacketsMapForIface.put(iface, transmittedPackets);
      }
    } catch (IOException e) {
      log.error("Meets error when reading {} for net status", NET_STATUS_PATH, e);
    }
  }
}
