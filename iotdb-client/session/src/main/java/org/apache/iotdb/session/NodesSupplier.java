package org.apache.iotdb.session;

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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodesSupplier implements INodeSupplier, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodesSupplier.class);

  private static final long UPDATE_PERIOD_IN_S = 60;
  private static final String SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";

  private static final String STATUS_COLUMN_NAME = "Status";

  private static final String IP_COLUMN_NAME = "RpcAddress";

  private static final String PORT_COLUMN_NAME = "RpcPort";

  private static final String REMOVING_STATUS = "Removing";

  // it's ok that TIMEOUT_IN_MS is larger than UPDATE_PERIOD_IN_S, because the next update request
  // won't be scheduled until last time is done.
  private static final long TIMEOUT_IN_MS = 60_000;

  private static final int FETCH_SIZE = 10_000;

  // availableNodes won't be updated frequently, so we use CopyOnWriteArrayList which is thread-safe
  // and is optimized for scenarios of reading more and writing less
  @SuppressWarnings("java:S3077")
  private volatile List<TEndPoint> availableNodes = new CopyOnWriteArrayList<>();

  private final boolean useSSL;
  private final String trustStore;
  private final String trustStorePwd;
  private final boolean enableRPCCompression;
  private final String userName;

  private final String password;

  private final ZoneId zoneId;

  private final int thriftDefaultBufferSize;

  private final int thriftMaxFrameSize;

  private final int connectionTimeoutInMs;

  private final String version;

  private final QueryEndPointPolicy policy = new RoundRobinPolicy();

  private ThriftConnection client;

  private volatile boolean closed = false;

  @SuppressWarnings("unsafeThreadSchedule")
  public static NodesSupplier createNodeSupplier(
      List<TEndPoint> endPointList,
      ScheduledExecutorService executorService,
      String userName,
      String password,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      int connectionTimeoutInMs,
      boolean useSSL,
      String trustStore,
      String trustStorePwd,
      boolean enableRPCCompression,
      String version) {

    NodesSupplier nodesSupplier =
        new NodesSupplier(
            endPointList,
            userName,
            password,
            zoneId,
            thriftDefaultBufferSize,
            thriftMaxFrameSize,
            connectionTimeoutInMs,
            useSSL,
            trustStore,
            trustStorePwd,
            enableRPCCompression,
            version);

    // call executorService.scheduleAtFixedRate here in a separate line
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    executorService.scheduleAtFixedRate(nodesSupplier, 0, UPDATE_PERIOD_IN_S, TimeUnit.SECONDS);

    return nodesSupplier;
  }

  private NodesSupplier(
      List<TEndPoint> endPointList,
      String userName,
      String password,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      int connectionTimeoutInMs,
      boolean useSSL,
      String trustStore,
      String trustStorePwd,
      boolean enableRPCCompression,
      String version) {
    this.availableNodes.addAll(new HashSet<>(endPointList));
    this.userName = userName;
    this.password = password;
    this.useSSL = useSSL;
    this.trustStore = trustStore;
    this.trustStorePwd = trustStorePwd;
    this.enableRPCCompression = enableRPCCompression;
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.version = version;
  }

  // the method will only be called while reconnecting, so it's ok to copy set each time
  // and the List needn't be thread-safe, because it will only be used in one thread.
  @Override
  public List<TEndPoint> get() {
    return availableNodes;
  }

  @Override
  public void run() {
    if (closed) {
      if (client != null) {
        destroyCurrentClient();
      }
      return;
    }
    if (client == null) {
      for (TEndPoint endPoint : availableNodes) {
        if (createConnection(endPoint)) {
          break;
        }
      }
    }

    // make sure the following code block can run thread-safely with close() method.
    synchronized (this) {
      if (client != null && !updateDataNodeList()) {
        destroyCurrentClient();
      }
    }
  }

  private boolean createConnection(TEndPoint endPoint) {
    client =
        new ThriftConnection(
            endPoint, thriftDefaultBufferSize, thriftMaxFrameSize, connectionTimeoutInMs);
    try {
      client.init(
          useSSL,
          trustStore,
          trustStorePwd,
          userName,
          password,
          enableRPCCompression,
          zoneId,
          version);
      return true;
    } catch (Exception e) {
      LOGGER.warn("Failed to create connection with {}.", endPoint);
      destroyCurrentClient();
      return false;
    }
  }

  private synchronized void destroyCurrentClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Override
  public void close() {
    closed = true;
    destroyCurrentClient();
  }

  @Override
  public Optional<TEndPoint> getQueryEndPoint() {
    if (availableNodes == null || availableNodes.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(policy.chooseOne(get()));
    }
  }

  private boolean updateDataNodeList() {
    try (SessionDataSet sessionDataSet =
        client.executeQueryStatement(SHOW_DATA_NODES_COMMAND, TIMEOUT_IN_MS, FETCH_SIZE)) {
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      List<TEndPoint> res = new ArrayList<>();
      while (iterator.next()) {
        String ip = iterator.getString(IP_COLUMN_NAME);
        // ignore 0.0.0.0 and removing DN
        if (!REMOVING_STATUS.equals(iterator.getString(STATUS_COLUMN_NAME))
            && !"0.0.0.0".equals(ip)) {
          String port = iterator.getString(PORT_COLUMN_NAME);
          if (ip != null && port != null) {
            res.add(new TEndPoint(ip, Integer.parseInt(port)));
          }
        }
      }
      // replace the older ones.
      if (!res.isEmpty()) {
        availableNodes = res;
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch data node list from {}.", client.endPoint);
      return false;
    }
  }
}
