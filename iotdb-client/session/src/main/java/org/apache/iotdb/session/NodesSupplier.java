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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class NodesSupplier implements INodeSupplier, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodesSupplier.class);

  private static final long UPDATE_PERIOD_IN_S = 60;
  private static final String SHOW_AVAILABLE_URLS_COMMAND = "SHOW AVAILABLE URLS";

  private static final String IP_COLUMN_NAME = "RpcAddress";

  private static final String PORT_COLUMN_NAME = "RpcPort";

  // it's ok that TIMEOUT_IN_MS is larger than UPDATE_PERIOD_IN_S, because the next update request
  // won't be scheduled until last time is done.
  private static final long TIMEOUT_IN_MS = 60_000;

  private static final int FETCH_SIZE = 10_000;

  // Publish complete snapshots; discovery must never overwrite the initial recovery addresses.
  @SuppressWarnings("java:S3077")
  private volatile List<TEndPoint> availableNodes;

  private final List<TEndPoint> seedNodes;
  private final ScheduledExecutorService executorService;
  private final AtomicBoolean refreshRequested = new AtomicBoolean();

  private final boolean useSSL;
  private final String trustStore;
  private final String trustStorePwd;
  private final String keyStore;
  private final String keyStorePwd;
  private final String sslProtocol;
  private final boolean enableRPCCompression;
  private final String userName;

  private final String password;

  private final ZoneId zoneId;

  private final int thriftDefaultBufferSize;

  private final int thriftMaxFrameSize;

  private final int connectionTimeoutInMs;

  private final String version;

  private final QueryEndPointPolicy policy = new RoundRobinPolicy();

  // Refresh serialization is independent of the short client-publication/cleanup critical section.
  private final ReentrantLock refreshLock = new ReentrantLock();
  // Accessed under refreshLock, in failure order, until a discovery query succeeds.
  private final Set<TEndPoint> queryFailedNodes = new LinkedHashSet<>();
  private final Object clientLock = new Object();
  private ThriftConnection client;
  private ThriftConnection activeClient;

  private volatile boolean closed = false;

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
      String keyStore,
      String keyStorePwd,
      String sslProtocol,
      boolean enableRPCCompression,
      String version) {

    return createNodeSupplier(
        endPointList,
        endPointList,
        executorService,
        userName,
        password,
        zoneId,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        connectionTimeoutInMs,
        useSSL,
        trustStore,
        trustStorePwd,
        keyStore,
        keyStorePwd,
        sslProtocol,
        enableRPCCompression,
        version);
  }

  /** Bootstrap discovery independently of the permanent recovery seeds (for Session reopen). */
  @SuppressWarnings("unsafeThreadSchedule")
  public static NodesSupplier createNodeSupplier(
      List<TEndPoint> endPointList,
      List<TEndPoint> seedNodes,
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
      String keyStore,
      String keyStorePwd,
      String sslProtocol,
      boolean enableRPCCompression,
      String version) {

    NodesSupplier nodesSupplier =
        new NodesSupplier(
            endPointList,
            seedNodes,
            executorService,
            userName,
            password,
            zoneId,
            thriftDefaultBufferSize,
            thriftMaxFrameSize,
            connectionTimeoutInMs,
            useSSL,
            trustStore,
            trustStorePwd,
            keyStore,
            keyStorePwd,
            sslProtocol,
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
      List<TEndPoint> seedNodes,
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
      String keyStore,
      String keyStorePwd,
      String sslProtocol,
      boolean enableRPCCompression,
      String version) {
    this.seedNodes =
        Collections.unmodifiableList(
            seedNodes.stream().distinct().map(TEndPoint::deepCopy).collect(Collectors.toList()));
    this.availableNodes =
        Collections.unmodifiableList(
            endPointList.stream().distinct().map(TEndPoint::deepCopy).collect(Collectors.toList()));
    this.executorService = executorService;
    this.userName = userName;
    this.password = password;
    this.useSSL = useSSL;
    this.trustStore = trustStore;
    this.trustStorePwd = trustStorePwd;
    this.keyStore = keyStore;
    this.keyStorePwd = keyStorePwd;
    this.sslProtocol = sslProtocol;
    this.enableRPCCompression = enableRPCCompression;
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.version = version;
  }

  // Readers retain an immutable snapshot while discovery publishes replacements.
  @Override
  public List<TEndPoint> get() {
    return availableNodes;
  }

  @Override
  public List<TEndPoint> getSeedNodes() {
    return seedNodes;
  }

  @Override
  public void requestRefresh() {
    if (!closed && refreshRequested.compareAndSet(false, true)) {
      try {
        executorService.execute(
            () -> {
              try {
                run();
              } finally {
                refreshRequested.set(false);
              }
            });
      } catch (RejectedExecutionException e) {
        // The session or pool may have shut down its executor concurrently.
        refreshRequested.set(false);
      }
    }
  }

  @Override
  public void run() {
    if (closed || !refreshLock.tryLock()) {
      return;
    }
    try {
      Set<TEndPoint> attempted = new HashSet<>();
      ThriftConnection current;
      synchronized (clientLock) {
        if (closed) {
          return;
        }
        current = client;
        activeClient = current;
      }
      if (current != null) {
        attempted.add(current.endPoint);
        if (refreshFrom(current)) {
          return;
        }
      }

      Set<TEndPoint> candidates = new LinkedHashSet<>(availableNodes);
      List<TEndPoint> seeds = new ArrayList<>(seedNodes);
      if (!seeds.isEmpty()) {
        Collections.rotate(seeds, -ThreadLocalRandom.current().nextInt(seeds.size()));
      }
      candidates.addAll(seeds);
      // Defer query-failed nodes across refresh rounds without losing them as recovery candidates.
      // Remember all failures so two failing nodes cannot starve a third, healthy candidate.
      queryFailedNodes.retainAll(candidates);
      candidates.removeAll(queryFailedNodes);
      candidates.addAll(queryFailedNodes);
      for (TEndPoint endPoint : candidates) {
        if (closed) {
          return;
        }
        if (attempted.add(endPoint)) {
          current = createConnection(endPoint);
          if (current != null && refreshFrom(current)) {
            return;
          }
        }
      }
    } finally {
      ThriftConnection toClose = null;
      synchronized (clientLock) {
        if (closed) {
          toClose = activeClient;
          client = null;
        }
        activeClient = null;
      }
      // Only the refresh owner releases buffers that an interrupted operation could still use.
      try {
        if (toClose != null) {
          toClose.close();
        }
      } finally {
        refreshLock.unlock();
      }
    }
  }

  /** Return false only when the transport failed and another candidate should be tried now. */
  private boolean refreshFrom(ThriftConnection current) {
    if (closed) {
      return true;
    }
    try {
      if (updateDataNodeList(current)) {
        queryFailedNodes.clear();
      } else {
        // End this round on a query error, but try another candidate on the next refresh.
        // Moving repeated failures to the end also permits recovery after a cluster-wide outage.
        queryFailedNodes.remove(current.endPoint);
        queryFailedNodes.add(current.endPoint);
        destroyClient(current);
      }
    } catch (IoTDBConnectionException e) {
      destroyClient(current);
      return false;
    }
    return true;
  }

  private ThriftConnection createConnection(TEndPoint endPoint) {
    ThriftConnection current =
        new ThriftConnection(
            endPoint, thriftDefaultBufferSize, thriftMaxFrameSize, connectionTimeoutInMs);
    synchronized (clientLock) {
      if (closed) {
        return null;
      }
      client = current;
      activeClient = current;
    }
    try {
      current.init(
          useSSL,
          trustStore,
          trustStorePwd,
          keyStore,
          keyStorePwd,
          sslProtocol,
          userName,
          password,
          enableRPCCompression,
          zoneId,
          version);
      if (closed) {
        destroyClient(current);
        return null;
      }
      return current;
    } catch (Exception e) {
      if (!closed) {
        LOGGER.warn(SessionMessages.FAILED_TO_CREATE_CONNECTION, endPoint);
      }
      destroyClient(current);
      return null;
    }
  }

  private void destroyClient(ThriftConnection current) {
    synchronized (clientLock) {
      if (client == current) {
        client = null;
      }
    }
    current.close();
  }

  @Override
  public void close() {
    ThriftConnection current;
    boolean inUse;
    synchronized (clientLock) {
      closed = true;
      current = client != null ? client : activeClient;
      inUse = current != null && current == activeClient;
      client = null;
    }
    if (current != null) {
      if (inUse) {
        current.abort();
      } else {
        current.close();
      }
    }
  }

  @Override
  public Optional<TEndPoint> getQueryEndPoint() {
    List<TEndPoint> snapshot = availableNodes;
    if (snapshot.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(policy.chooseOne(snapshot));
    }
  }

  private boolean updateDataNodeList(ThriftConnection current) throws IoTDBConnectionException {
    try (SessionDataSet sessionDataSet =
        current.executeQueryStatement(SHOW_AVAILABLE_URLS_COMMAND, TIMEOUT_IN_MS, FETCH_SIZE)) {
      updateAvailableNodes(sessionDataSet);
    } catch (IoTDBConnectionException e) {
      if (!closed) {
        LOGGER.warn(SessionMessages.FAILED_TO_FETCH_DATA_NODE_LIST, current.endPoint);
      }
      if (!(e.getCause() instanceof TApplicationException)) {
        throw e;
      }
      // A Thrift application error also means the peer responded; it is not a transport failure.
      return false;
    } catch (Exception e) {
      if (!closed) {
        LOGGER.warn(SessionMessages.FAILED_TO_FETCH_DATA_NODE_LIST, current.endPoint);
      }
      return false;
    }
    return true;
  }

  private void updateAvailableNodes(SessionDataSet sessionDataSet) throws Exception {
    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
    List<TEndPoint> res = new ArrayList<>();
    while (iterator.next()) {
      String ip = iterator.getString(IP_COLUMN_NAME);
      // ignore wildcard addresses
      if (!UrlUtils.isWildcardAddress(ip)) {
        String port = iterator.getString(PORT_COLUMN_NAME);
        if (ip != null && port != null) {
          res.add(new TEndPoint(ip, Integer.parseInt(port)));
        }
      }
    }
    // Wildcard addresses and transient Unknown statuses after leader changes convey no usable
    // discovery information. Retain the last snapshot for routing and recovery in both cases.
    if (!closed && !res.isEmpty()) {
      availableNodes = Collections.unmodifiableList(res);
    }
  }
}
