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

package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSender;
import org.apache.iotdb.common.rpc.thrift.TServiceType;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.TestConnectionUtils;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.dn.DnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.db.protocol.client.dn.DnToDnRequestType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** */
public class ConnectivityService implements IService {
  private static final long CONNECTION_PROBING_INTERVAL_MS = 5_000L;
  private static final long PROBING_TIMEOUT_MS = 1_000L;

  /*
   * This field holds the cluster-wise topology from ConfigNode.
   * It is only updated via exchanging heartbeat with ConfigNodes.
   */
  private Map<TDataNodeLocation, Set<TDataNodeLocation>> clusterTopology = new HashMap<>();

  private final TDataNodeLocation myself =
      IoTDBDescriptor.getInstance().getConfig().generateLocalDataNodeLocation();

  private final ScheduledExecutorService executor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_CONNECTIVITY_SERVICE.getName());

  /*
   * This field holds this node's local connectivity view with other DataNodes.
   * It is updated via background connection probing.
   */
  private Map<TDataNodeLocation, Boolean> localConnectivityMap = new HashMap<>();
  private final AtomicBoolean localStatusChanged = new AtomicBoolean(false);

  @Override
  public void start() throws StartupException {
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        executor,
        this::connectionProbing,
        0L,
        CONNECTION_PROBING_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    try {
      executor.awaitTermination(CONNECTION_PROBING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException i) {
      Thread.currentThread().interrupt();
    } finally {
      executor.shutdown();
    }
  }

  private synchronized void connectionProbing() {
    final List<TDataNodeLocation> allDataNodesToSend =
        new ArrayList<>(getClusterTopology().keySet());
    allDataNodesToSend.remove(myself);

    final TSender sender = new TSender().setDataNodeLocation(myself);

    final List<TTestConnectionResult> connectionResults =
        TestConnectionUtils.testConnectionsImpl(
            allDataNodesToSend,
            sender,
            TDataNodeLocation::getDataNodeId,
            TDataNodeLocation::getInternalEndPoint,
            TServiceType.DataNodeInternalService,
            DnToDnRequestType.TEST_CONNECTION,
            (AsyncRequestContext<Object, TSStatus, DnToDnRequestType, TDataNodeLocation> handler) ->
                DnToDnInternalServiceAsyncRequestManager.getInstance()
                    .sendAsyncRequestWithTimeoutInMs(handler, PROBING_TIMEOUT_MS));

    final Map<TEndPoint, TDataNodeLocation> reversedMap = new HashMap<>();
    allDataNodesToSend.forEach(loc -> reversedMap.put(loc.getInternalEndPoint(), loc));

    final Map<TDataNodeLocation, Boolean> localConnectionsCurrent = new HashMap<>();
    connectionResults.forEach(
        r ->
            localConnectivityMap.put(
                reversedMap.get(r.getServiceProvider().getEndPoint()), r.isSuccess()));

    // compare the current and the last
    if (!localConnectionsCurrent.equals(localConnectivityMap)) {
      localStatusChanged.set(true);
      localConnectivityMap = localConnectionsCurrent;
    }
  }

  public Optional<Map<TDataNodeLocation, Boolean>> getLocalConnectivity() {
    if (localStatusChanged.compareAndSet(true, false)) {
      return Optional.of(Collections.unmodifiableMap(localConnectivityMap));
    }
    return Optional.empty();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONNECTIVITY_SERVICE;
  }

  public void updateClusterTopology(Map<TDataNodeLocation, Set<TDataNodeLocation>> latestMap) {
    this.clusterTopology = latestMap;
  }

  public Map<TDataNodeLocation, Set<TDataNodeLocation>> getClusterTopology() {
    return Collections.unmodifiableMap(clusterTopology);
  }

  private static class ConnectivityServiceHolder {
    private static final ConnectivityService INSTANCE = new ConnectivityService();

    private ConnectivityServiceHolder() {}
  }

  public static ConnectivityService getInstance() {
    return ConnectivityServiceHolder.INSTANCE;
  }
}
