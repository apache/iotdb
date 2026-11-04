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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.AbstractConfigNodeInfo;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.db.conf.DataNodeSystemPropertiesHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigNodeInfo extends AbstractConfigNodeInfo {
  private static final String NODE_TYPE_NAME = "datanode";

  private final Map<TEndPoint, Integer> configNodeIdMap = new HashMap<>();

  private ConfigNodeInfo() {
    this(DataNodeSystemPropertiesHandler.getInstance());
  }

  private ConfigNodeInfo(final SystemPropertiesHandler systemPropertiesHandler) {
    super(systemPropertiesHandler);
  }

  public static void reinitializeStatics() {
    ConfigNodeInfoHolder.INSTANCE = new ConfigNodeInfo();
  }

  static void reinitializeStatics(final SystemPropertiesHandler systemPropertiesHandler) {
    ConfigNodeInfoHolder.INSTANCE = new ConfigNodeInfo(systemPropertiesHandler);
  }

  @Override
  public synchronized boolean updateConfigNodeList(final List<TEndPoint> latestConfigNodes) {
    if (!super.updateConfigNodeList(latestConfigNodes)) {
      return false;
    }
    configNodeIdMap.keySet().retainAll(latestConfigNodes);
    return true;
  }

  public synchronized boolean updateConfigNodeLocations(
      final List<TConfigNodeLocation> latestConfigNodeLocations) {
    if (latestConfigNodeLocations == null) {
      return false;
    }

    final List<TEndPoint> latestConfigNodes = new ArrayList<>();
    final Map<TEndPoint, Integer> latestConfigNodeIdMap = new HashMap<>();
    for (final TConfigNodeLocation configNodeLocation : latestConfigNodeLocations) {
      if (configNodeLocation == null || configNodeLocation.getInternalEndPoint() == null) {
        continue;
      }
      final TEndPoint internalEndPoint = configNodeLocation.getInternalEndPoint();
      latestConfigNodes.add(internalEndPoint);
      latestConfigNodeIdMap.put(internalEndPoint, configNodeLocation.getConfigNodeId());
    }

    if (!updateConfigNodeList(latestConfigNodes)) {
      return false;
    }
    configNodeIdMap.putAll(latestConfigNodeIdMap);
    return true;
  }

  @Override
  protected String getNodeTypeName() {
    return NODE_TYPE_NAME;
  }

  public synchronized int getConfigNodeId(final TEndPoint internalEndPoint) {
    if (internalEndPoint == null) {
      return -1;
    }
    return configNodeIdMap.getOrDefault(internalEndPoint, -1);
  }

  private static class ConfigNodeInfoHolder {
    private static ConfigNodeInfo INSTANCE = new ConfigNodeInfo();

    private ConfigNodeInfoHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeInfo getInstance() {
    return ConfigNodeInfoHolder.INSTANCE;
  }
}
