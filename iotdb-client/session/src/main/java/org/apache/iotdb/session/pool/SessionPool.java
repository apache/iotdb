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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.session.DummyNodesSupplier;
import org.apache.iotdb.session.NodesSupplier;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * SessionPool is a wrapper of a Session Set. Using SessionPool, the user do not need to consider
 * how to reuse a session connection. Even if the session is disconnected, the session pool can
 * recognize it and remove the broken session connection and create a new one.
 *
 * <p>If there is no available connections and the pool reaches its max size, the all methods will
 * hang until there is a available connection.
 *
 * <p>If a user has waited for a session for more than 60 seconds, a warn log will be printed.
 *
 * <p>The only thing you have to remember is that:
 *
 * <p>For a query, if you have get all data, i.e., SessionDataSetWrapper.hasNext() == false, it is
 * ok. Otherwise, i.e., you want to stop the query before you get all data
 * (SessionDataSetWrapper.hasNext() == true), then you have to call
 * closeResultSet(SessionDataSetWrapper wrapper) manually. Otherwise the connection is occupied by
 * the query.
 *
 * <p>Another case that you have to manually call closeResultSet() is that when there is exception
 * when you call SessionDataSetWrapper.hasNext() or next()
 */

// ignore Generic exceptions & Throwable should never be throw, ignore Either log or rethrow this
// exception.
@SuppressWarnings({"squid:S112", "java:S1181", "java:S2139"})
public class SessionPool implements ISessionPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPool.class);
  public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
  public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";

  private static final int RETRY = 3;
  private static final int FINAL_RETRY = RETRY - 1;

  private final ConcurrentLinkedDeque<ISession> queue = new ConcurrentLinkedDeque<>();
  // for session whose resultSet is not released.
  private final ConcurrentMap<ISession, ISession> occupied = new ConcurrentHashMap<>();
  private int size = 0;
  private int maxSize = 0;
  private final long waitToGetSessionTimeoutInMs;

  // parameters for Session constructor
  private final String host;
  private final int port;
  private final String user;
  private final String password;
  private int fetchSize;

  private boolean useSSL;

  private String trustStore;

  private String trustStorePwd;
  private ZoneId zoneId;
  // this field only take effect in write request, nothing to do with any other type requests,
  // like query, load and so on.
  // if set to true, it means that we may redirect the write request to its corresponding leader
  // if set to false, it means that we will only send write request to first available DataNode(it
  // may be changed while current DataNode is not available, for example, we may retry to connect
  // to another available DataNode)
  // so even if enableRedirection is set to false, we may also send write request to another
  // datanode while encountering retriable errors in current DataNode
  private boolean enableRedirection;
  private boolean enableRecordsAutoConvertTablet;
  private boolean enableQueryRedirection = false;

  private Map<String, TEndPoint> deviceIdToEndpoint;

  private int thriftDefaultBufferSize;
  private int thriftMaxFrameSize;

  /**
   * Timeout of query can be set by users. A negative number means using the default configuration
   * of server. And value 0 will disable the function of query timeout.
   */
  private long queryTimeoutInMs = -1;

  // The version number of the client which used for compatibility in the server
  private Version version;

  // parameters for Session#open()
  private final int connectionTimeoutInMs;
  private final boolean enableCompression;

  // whether the queue is closed.
  private boolean closed;

  // Redirect-able SessionPool
  private final List<String> nodeUrls;

  // formatted nodeUrls for logging e.g. "host:port" or "[host:port, host:port, host:port]"
  private final String formattedNodeUrls;

  // used to update datanodeList periodically
  private volatile ScheduledExecutorService executorService;

  private INodeSupplier availableNodes;

  // set to true, means that we will start a background thread to fetch all available (Status is
  // not Removing) datanodes in cluster, and these available nodes will be used in retrying stage
  private boolean enableAutoFetch = true;

  // max retry count, if set to 0, means that we won't do any retry
  // we can use any available DataNodes(fetched in background thread if enableAutoFetch is true,
  // or nodeUrls user specified) to retry, even if enableRedirection is false
  protected int maxRetryCount = SessionConfig.MAX_RETRY_COUNT;

  protected long retryIntervalInMs = SessionConfig.RETRY_INTERVAL_IN_MS;

  private static final String INSERT_RECORD_FAIL = "insertRecord failed";

  private static final String INSERT_RECORD_ERROR_MSG = "unexpected error in insertRecord";

  private static final String INSERT_RECORDS_ERROR_MSG = "unexpected error in insertRecords";

  private static final String EXECUTE_LASTDATAQUERY_FAIL = "executeLastDataQuery failed";

  private static final String EXECUTE_LASTDATAQUERY_ERROR =
      "unexpected error in executeLastDataQuery";

  private static final String EXECUTE_AGGREGATION_QUERY_FAIL = "executeAggregationQuery failed";

  private static final String INSERT_RECORDS_OF_ONE_DEVICE_ERROR_MSG =
      "unexpected error in insertRecordsOfOneDevice";

  private static final String DELETE_DATA_ERROR_MSG = "unexpected error in deleteData";

  private static final String CREATE_SCHEMA_TEMPLATE_ERROR_MSG =
      "unexpected error in createSchemaTemplate";

  private static final String EXECUTE_AGGREGATION_QUERY_ERROR_MSG =
      "unexpected error in executeAggregationQuery";

  private static final String DELETE_DATA_FAIL = "deleteData failed";

  private static final String INSERT_RECORDS_OF_ONE_DEVICE_FAIL = "insertRecordsOfOneDevice failed";

  private static final String CREATE_SCHEMA_TEMPLATE_FAIL = "createSchemaTemplate failed";

  public SessionPool(String host, int port, String user, String password, int maxSize) {
    this(
        host,
        port,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        false,
        null,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(List<String> nodeUrls, String user, String password, int maxSize) {
    this(
        nodeUrls,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        false,
        null,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, boolean enableCompression) {
    this(
        host,
        port,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        enableCompression,
        null,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, boolean enableCompression) {
    this(
        nodeUrls,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        enableCompression,
        null,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    this(
        host,
        port,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        enableCompression,
        null,
        enableRedirection,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    this(
        nodeUrls,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        enableCompression,
        null,
        enableRedirection,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, ZoneId zoneId) {
    this(
        host,
        port,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        false,
        zoneId,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, ZoneId zoneId) {
    this(
        nodeUrls,
        user,
        password,
        maxSize,
        SessionConfig.DEFAULT_FETCH_SIZE,
        60_000,
        false,
        zoneId,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
        SessionConfig.DEFAULT_VERSION,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  @SuppressWarnings("squid:S107")
  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    this.maxSize = maxSize;
    this.host = host;
    this.port = port;
    this.nodeUrls = null;
    this.user = user;
    this.password = password;
    this.fetchSize = fetchSize;
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
    this.enableCompression = enableCompression;
    this.zoneId = zoneId;
    this.enableRedirection = enableRedirection;
    if (this.enableRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
    }
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.version = version;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.formattedNodeUrls = String.format("%s:%s", host, port);
    initThreadPool();
    initAvailableNodes(Collections.singletonList(new TEndPoint(host, port)));
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean useSSL,
      String trustStore,
      String trustStorePwd) {
    this.maxSize = maxSize;
    this.host = host;
    this.port = port;
    this.nodeUrls = null;
    this.user = user;
    this.password = password;
    this.fetchSize = fetchSize;
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
    this.enableCompression = enableCompression;
    this.zoneId = zoneId;
    this.enableRedirection = enableRedirection;
    if (this.enableRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
    }
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.version = version;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.formattedNodeUrls = String.format("%s:%s", host, port);
    this.useSSL = useSSL;
    this.trustStore = trustStore;
    this.trustStorePwd = trustStorePwd;
    initThreadPool();
    initAvailableNodes(Collections.singletonList(new TEndPoint(host, port)));
  }

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    this.maxSize = maxSize;
    this.host = null;
    this.port = -1;
    if (nodeUrls.isEmpty()) {
      throw new IllegalArgumentException("nodeUrls shouldn't be empty.");
    }
    this.nodeUrls = nodeUrls;
    this.user = user;
    this.password = password;
    this.fetchSize = fetchSize;
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
    this.enableCompression = enableCompression;
    this.zoneId = zoneId;
    this.enableRedirection = enableRedirection;
    if (this.enableRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
    }
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.version = version;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.formattedNodeUrls = nodeUrls.toString();
    initThreadPool();
    initAvailableNodes(SessionUtils.parseSeedNodeUrls(nodeUrls));
  }

  public SessionPool(Builder builder) {
    this.maxSize = builder.maxSize;
    this.user = builder.user;
    this.password = builder.pw;
    this.fetchSize = builder.fetchSize;
    this.waitToGetSessionTimeoutInMs = builder.waitToGetSessionTimeoutInMs;
    this.enableCompression = builder.enableCompression;
    this.zoneId = builder.zoneId;
    this.enableRedirection = builder.enableRedirection;
    if (this.enableRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
    }
    this.enableRecordsAutoConvertTablet = builder.enableRecordsAutoConvertTablet;
    this.connectionTimeoutInMs = builder.connectionTimeoutInMs;
    this.version = builder.version;
    this.thriftDefaultBufferSize = builder.thriftDefaultBufferSize;
    this.thriftMaxFrameSize = builder.thriftMaxFrameSize;
    this.enableAutoFetch = builder.enableAutoFetch;
    this.useSSL = builder.useSSL;
    this.trustStore = builder.trustStore;
    this.trustStorePwd = builder.trustStorePwd;
    this.maxRetryCount = builder.maxRetryCount;
    this.retryIntervalInMs = builder.retryIntervalInMs;
    this.queryTimeoutInMs = builder.queryTimeoutInMs;

    if (enableAutoFetch) {
      initThreadPool();
    }
    if (builder.nodeUrls != null) {
      if (builder.nodeUrls.isEmpty()) {
        throw new IllegalArgumentException("nodeUrls shouldn't be empty.");
      }
      this.nodeUrls = builder.nodeUrls;
      this.host = null;
      this.port = -1;
      this.formattedNodeUrls = builder.nodeUrls.toString();
      if (enableAutoFetch) {
        initAvailableNodes(SessionUtils.parseSeedNodeUrls(nodeUrls));
      } else {
        this.availableNodes = new DummyNodesSupplier(SessionUtils.parseSeedNodeUrls(nodeUrls));
      }

    } else {
      this.host = builder.host;
      this.port = builder.port;
      this.nodeUrls = null;
      this.formattedNodeUrls = String.format("%s:%s", host, port);
      if (enableAutoFetch) {
        initAvailableNodes(Collections.singletonList(new TEndPoint(host, port)));
      } else {
        this.availableNodes =
            new DummyNodesSupplier(Collections.singletonList(new TEndPoint(host, port)));
      }
    }
  }

  private Session constructNewSession() {
    Session session;
    if (nodeUrls == null) {
      // Construct custom Session
      session =
          new Session.Builder()
              .host(host)
              .port(port)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .enableRecordsAutoConvertTablet(enableRecordsAutoConvertTablet)
              .version(version)
              .useSSL(useSSL)
              .trustStore(trustStore)
              .trustStorePwd(trustStorePwd)
              .maxRetryCount(maxRetryCount)
              .retryIntervalInMs(retryIntervalInMs)
              .timeOut(queryTimeoutInMs)
              .build();
    } else {
      // Construct redirect-able Session
      session =
          new Session.Builder()
              .nodeUrls(nodeUrls)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .enableRecordsAutoConvertTablet(enableRecordsAutoConvertTablet)
              .version(version)
              .useSSL(useSSL)
              .trustStore(trustStore)
              .trustStorePwd(trustStorePwd)
              .maxRetryCount(maxRetryCount)
              .retryIntervalInMs(retryIntervalInMs)
              .timeOut(queryTimeoutInMs)
              .build();
    }
    session.setEnableQueryRedirection(enableQueryRedirection);
    return session;
  }

  private void initThreadPool() {
    this.executorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "PeriodicalUpdateDNList", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
  }

  private void initAvailableNodes(List<TEndPoint> endPointList) {
    this.availableNodes =
        NodesSupplier.createNodeSupplier(
            endPointList,
            executorService,
            user,
            password,
            zoneId,
            thriftDefaultBufferSize,
            thriftMaxFrameSize,
            connectionTimeoutInMs,
            useSSL,
            trustStore,
            trustStorePwd,
            enableCompression,
            version.toString());
  }

  // if this method throws an exception, either the server is broken, or the ip/port/user/password
  // is incorrect.
  @SuppressWarnings({"squid:S3776", "squid:S2446"}) // Suppress high Cognitive Complexity warning
  private ISession getSession() throws IoTDBConnectionException {
    ISession session = queue.poll();
    if (closed) {
      throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
    }
    if (session != null) {
      return session;
    }

    boolean shouldCreate = false;

    long start = System.currentTimeMillis();
    while (session == null) {
      synchronized (this) {
        if (size < maxSize) {
          // we can create more session
          size++;
          shouldCreate = true;
          // but we do it after skip synchronized block because connection a session is time
          // consuming.
          break;
        }

        // we have to wait for someone returns a session.
        try {
          this.wait(1000);
          long timeOut = Math.min(waitToGetSessionTimeoutInMs, 60_000);
          if (System.currentTimeMillis() - start > timeOut) {
            LOGGER.warn(
                "the SessionPool has wait for {} seconds to get a new connection: {} with {}, {}",
                (System.currentTimeMillis() - start) / 1000,
                formattedNodeUrls,
                user,
                password);
            LOGGER.warn(
                "current occupied size {}, queue size {}, considered size {} ",
                occupied.size(),
                queue.size(),
                size);
            if (System.currentTimeMillis() - start > waitToGetSessionTimeoutInMs) {
              throw new IoTDBConnectionException(
                  String.format("timeout to get a connection from %s", formattedNodeUrls));
            }
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted!", e);
          Thread.currentThread().interrupt();
          // wake up from this.wait(1000) by this.notify()
        }

        session = queue.poll();

        // for putBack or size--
        this.notify();

        if (closed) {
          throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
        }
      }
    }

    if (shouldCreate) {
      // create a new one.

      session = constructNewSession();

      try {
        session.open(enableCompression, connectionTimeoutInMs, deviceIdToEndpoint, availableNodes);
        // avoid someone has called close() the session pool
        synchronized (this) {
          if (closed) {
            // have to release the connection...
            session.close();
            throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
          }
        }
      } catch (IoTDBConnectionException e) {
        // if exception, we will throw the exception.
        // Meanwhile, we have to set size--
        synchronized (this) {
          size--;
          // we do not need to notifyAll as any waited thread can continue to work after waked up.
          this.notify();
        }
        throw e;
      }
    }

    return session;
  }

  @Override
  public int currentAvailableSize() {
    return queue.size();
  }

  @Override
  public int currentOccupiedSize() {
    return occupied.size();
  }

  @SuppressWarnings({"squid:S2446"})
  private void putBack(ISession session) {
    queue.push(session);
    synchronized (this) {
      // we do not need to notifyAll as any waited thread can continue to work after waked up.
      this.notify();
      // comment the following codes as putBack is too frequently called.
      //      if (logger.isTraceEnabled()) {
      //        logger.trace("put a session back and notify others..., queue.size = {}",
      // queue.size());
      //      }
    }
  }

  private void occupy(ISession session) {
    occupied.put(session, session);
  }

  /** close all connections in the pool */
  @Override
  public synchronized void close() {
    for (ISession session : queue) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        // do nothing
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    for (ISession session : occupied.keySet()) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        // do nothing
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    if (this.executorService != null) {
      this.executorService.shutdown();
      this.executorService = null;
    }
    if (availableNodes != null) {
      this.availableNodes.close();
      this.availableNodes = null;
    }
    LOGGER.info("closing the session pool, cleaning queues...");
    this.closed = true;
    queue.clear();
    occupied.clear();
  }

  @Override
  public void closeResultSet(SessionDataSetWrapper wrapper) {
    boolean putback = true;
    try {
      wrapper.getSessionDataSet().closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      tryConstructNewSession();
      putback = false;
    } finally {
      ISession session = occupied.remove(wrapper.getSession());
      if (putback && session != null) {
        putBack(wrapper.getSession());
      }
    }
  }

  @SuppressWarnings({"squid:S2446"})
  private void tryConstructNewSession() {
    Session session = constructNewSession();
    try {
      session.open(enableCompression, connectionTimeoutInMs, deviceIdToEndpoint, availableNodes);
      // avoid someone has called close() the session pool
      synchronized (this) {
        if (closed) {
          // have to release the connection...
          session.close();
          throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
        }
        queue.push(session);
        this.notify();
      }
    } catch (IoTDBConnectionException e) {
      synchronized (this) {
        size--;
        // we do not need to notifyAll as any waited thread can continue to work after waked up.
        this.notify();
      }
    }
  }

  private void closeSession(ISession session) {
    if (session != null) {
      try {
        session.close();
      } catch (Exception e2) {
        // do nothing. We just want to guarantee the session is closed.
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e2);
      }
    }
  }

  private void cleanSessionAndMayThrowConnectionException(
      ISession session, int times, IoTDBConnectionException e) throws IoTDBConnectionException {
    closeSession(session);
    tryConstructNewSession();
    if (times == FINAL_RETRY) {
      throw new IoTDBConnectionException(
          String.format(
              "retry to execute statement on %s failed %d times: %s",
              formattedNodeUrls, RETRY, e.getMessage()),
          e);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * @param tablet data batch
   */
  @Override
  public void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    /*
     *  A Tablet example:
     *        device1
     *     time s1, s2, s3
     *     1,   1,  1,  1
     *     2,   2,  2,  2
     *     3,   3,  3,  3
     *
     * times in Tablet may be not in ascending orde
     */
    insertTablet(tablet, false);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   *
   * @param tablet a tablet data of one device
   * @param sorted whether times in Tablet are in ascending order
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be throw
  @Override
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    /*
     *  A Tablet example:
     *        device1
     *     time s1, s2, s3
     *     1,   1,  1,  1
     *     2,   2,  2,  2
     *     3,   3,  3,  3
     */

    ISession session = getSession();
    try {
      session.insertTablet(tablet, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertTablet failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertTablet", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   *
   * @param tablet a tablet data of one device
   */
  @Override
  public void insertAlignedTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedTablet(tablet, false);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   *
   * @param tablet a tablet data of one device
   * @param sorted whether times in Tablet are in ascending order
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be throw
  @Override
  public void insertAlignedTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedTablet(tablet, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedTablet failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedTablet", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  @Override
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedTablets(tablets, false);
  }

  /**
   * use batch interface to insert aligned data
   *
   * @param tablets multiple batch
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be throw
  @Override
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertTablets(tablets, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertTablets failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertTablets", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * use batch interface to insert aligned data
   *
   * @param tablets multiple batch
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be throw
  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedTablets(tablets, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedTablets failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedTablets", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be throw
  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data in batch format, which can reduce the overhead of network. This method is
   * just like jdbc batch insert, we pack some insert request in batch and send them to server. If
   * you want to improve your performance, please see insertTablet method.
   *
   * @see Session#insertTablet(Tablet)
   */
  @SuppressWarnings({"squid:S112"}) // ignore Generic exceptions should never be thrown
  @Override
  public void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecords(
          multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecords", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, false);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORDS_OF_ONE_DEVICE_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_OF_ONE_DEVICE_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   * @deprecated
   */
  @Deprecated
  @Override
  public void insertOneDeviceRecords(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, false);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORDS_OF_ONE_DEVICE_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_OF_ONE_DEVICE_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert String format data that belong to the same device in batch format, which can reduce the
   * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
   * batch and send them to server If you want improve your performance, please see insertTablet
   * method
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList, false);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertStringRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertStringRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, haveSorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORDS_OF_ONE_DEVICE_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_OF_ONE_DEVICE_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   * @deprecated
   */
  @Override
  @Deprecated
  public void insertOneDeviceRecords(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, haveSorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORDS_OF_ONE_DEVICE_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_OF_ONE_DEVICE_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert String format data that belong to the same device in batch format, which can reduce the
   * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
   * batch and send them to server If you want improve your performance, please see insertTablet
   * method
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertStringRecordsOfOneDevice(
          deviceId, times, measurementsList, valuesList, haveSorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertStringRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertStringRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data that belong to the same device in batch format, which can reduce the
   * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
   * batch and send them to server If you want improve your performance, please see insertTablet
   * method.
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, false);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data as String format that belong to the same device in batch format, which can
   * reduce the overhead of network. This method is just like jdbc batch insert, we pack some insert
   * request in batch and send them to server If you want improve your performance, please see
   * insertTablet method.
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedStringRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedStringRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data that belong to the same device in batch format, which can reduce the
   * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
   * batch and send them to server If you want improve your performance, please see insertTablet
   * method.
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, haveSorted);
      putBack(session);
      return;
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data as String format that belong to the same device in batch format, which can
   * reduce the overhead of network. This method is just like jdbc batch insert, we pack some insert
   * request in batch and send them to server If you want improve your performance, please see
   * insertTablet method.
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedStringRecordsOfOneDevice(
          deviceId, times, measurementsList, valuesList, haveSorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedStringRecordsOfOneDevice failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedStringRecordsOfOneDevice", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecords(deviceIds, times, measurementsList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORDS_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * Insert aligned data in batch format, which can reduce the overhead of network. This method is
   * just like jdbc batch insert, we pack some insert request in batch and send them to server If
   * you want improve your performance, please see insertTablet method.
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecords(
          multiSeriesIds, times, multiMeasurementComponentsList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecords", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecord(deviceId, time, measurements, types, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.error(INSERT_RECORD_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORD_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecord(deviceId, time, measurements, types, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORD_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORD_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getTimestampPrecision()
      throws IoTDBConnectionException, StatementExecutionException {
    String timestampPrecision = "ms";
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        timestampPrecision = session.getTimestampPrecision();
        putBack(session);
        return timestampPrecision;
      } catch (TException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("getTimestampPrecision failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, new IoTDBConnectionException(e));
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in getTimestampPrecision", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return timestampPrecision;
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecord(
      String multiSeriesId,
      long time,
      List<String> multiMeasurementComponents,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, types, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecord failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecord", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertRecord(deviceId, time, measurements, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(INSERT_RECORD_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(INSERT_RECORD_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecord(
      String multiSeriesId, long time, List<String> multiMeasurementComponents, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("insertAlignedRecord failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in insertAlignedRecord", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertTablet(tablet);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertTablet failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertTablet", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertTablet(tablet, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertTablet failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertTablet", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertTablets(tablets);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertTablets failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertTablets", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertTablets(tablets, sorted);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertTablets failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertTablets", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertRecords(deviceIds, times, measurementsList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertRecords", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertRecords failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertRecords", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertRecord(deviceId, time, measurements, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertRecord failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertRecord", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.testInsertRecord(deviceId, time, measurements, types, values);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("testInsertRecord failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in testInsertRecord", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  @Override
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.deleteTimeseries(path);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("deleteTimeseries failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in deleteTimeseries", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  @Override
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.deleteTimeseries(paths);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("deleteTimeseries failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in deleteTimeseries", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param time data with time stamp less than or equal to time will be deleted
   */
  @Override
  public void deleteData(String path, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.deleteData(path, time);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(DELETE_DATA_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(DELETE_DATA_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param time data with time stamp less than or equal to time will be deleted
   */
  @Override
  public void deleteData(List<String> paths, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.deleteData(paths, time);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(DELETE_DATA_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(DELETE_DATA_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * delete data >= startTime and data <= endTime in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param startTime delete range start time
   * @param endTime delete range end time
   */
  @Override
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    ISession session = getSession();
    try {
      session.deleteData(paths, startTime, endTime);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn(DELETE_DATA_FAIL, e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error(DELETE_DATA_ERROR_MSG, e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated Use {@link #createDatabase(String)} instead.
   */
  @Deprecated
  @Override
  public void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.setStorageGroup(storageGroupId);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("setStorageGroup failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in setStorageGroup", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @deprecated Use {@link #deleteDatabase(String)} instead.
   */
  @Deprecated
  @Override
  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.deleteStorageGroup(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteStorageGroup failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in deleteStorageGroup", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @deprecated Use {@link #deleteDatabases(List)} instead.
   */
  @Deprecated
  @Override
  public void deleteStorageGroups(List<String> storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.deleteStorageGroups(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteStorageGroups failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in deleteStorageGroups", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createDatabase(database);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createDatabase failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createDatabase", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void deleteDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.deleteDatabase(database);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteDatabase failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in deleteDatabase", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void deleteDatabases(List<String> databases)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.deleteDatabases(databases);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteDatabases failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in deleteDatabases", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createTimeseries(path, dataType, encoding, compressor);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createTimeseries", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createTimeseries(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createTimeseries(
            path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createTimeseries", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createAlignedTimeseries(
            deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createAlignedTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createAlignedTimeseries", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createAlignedTimeseries(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            tagsList,
            attributesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createAlignedTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createAlignedTimeseries", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createMultiTimeseries(
            paths,
            dataTypes,
            encodings,
            compressors,
            propsList,
            tagsList,
            attributesList,
            measurementAliasList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createMultiTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createMultiTimeseries", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        boolean resp = session.checkTimeseriesExists(path);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("checkTimeseriesExists failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in checkTimeseriesExists", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here.
    return false;
  }

  /**
   * Construct Template at session and create it at server.
   *
   * @see Template
   */
  @Override
  public void createSchemaTemplate(Template template)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createSchemaTemplate(template);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(CREATE_SCHEMA_TEMPLATE_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(CREATE_SCHEMA_TEMPLATE_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Create a template with flat measurements, not tree structured. Need to specify datatype,
   * encoding and compressor of each measurement, and alignment of these measurements at once.
   *
   * @oaram templateName name of template to create
   * @param measurements flat measurements of the template, cannot contain character dot
   * @param dataTypes datatype of each measurement in the template
   * @param encodings encodings of each measurement in the template
   * @param compressors compression type of each measurement in the template
   * @param isAligned specify whether these flat measurements are aligned
   */
  @Override
  public void createSchemaTemplate(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createSchemaTemplate(
            templateName, measurements, dataTypes, encodings, compressors, isAligned);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(CREATE_SCHEMA_TEMPLATE_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(CREATE_SCHEMA_TEMPLATE_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Compatible for rel/0.12, this method will create an unaligned flat template as a result. Notice
   * that there is no aligned concept in 0.12, so only the first measurement in each nested list
   * matters.
   *
   * @param name name of the template
   * @param schemaNames it works as a virtual layer inside template in 0.12, and makes no difference
   *     after 0.13
   * @param measurements the first measurement in each nested list will constitute the final flat
   *     template
   * @param dataTypes the data type of each measurement, only the first one in each nested list
   *     matters as above
   * @param encodings the encoding of each measurement, only the first one in each nested list
   *     matters as above
   * @param compressors the compressor of each measurement
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   * @deprecated
   */
  @Deprecated
  @Override
  public void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createSchemaTemplate(
            name, schemaNames, measurements, dataTypes, encodings, compressors);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(CREATE_SCHEMA_TEMPLATE_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(CREATE_SCHEMA_TEMPLATE_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void addAlignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.addAlignedMeasurementsInTemplate(
            templateName, measurementsPath, dataTypes, encodings, compressors);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addAlignedMeasurementsInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in addAlignedMeasurementsInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void addAlignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.addAlignedMeasurementInTemplate(
            templateName, measurementPath, dataType, encoding, compressor);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addAlignedMeasurementInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in addAlignedMeasurementInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void addUnalignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.addUnalignedMeasurementsInTemplate(
            templateName, measurementsPath, dataTypes, encodings, compressors);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addUnalignedMeasurementsInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in addUnalignedMeasurementsInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void addUnalignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.addUnalignedMeasurementInTemplate(
            templateName, measurementPath, dataType, encoding, compressor);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addUnalignedMeasurementInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in addUnalignedMeasurementInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void deleteNodeInTemplate(String templateName, String path)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.deleteNodeInTemplate(templateName, path);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteNodeInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in deleteNodeInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public int countMeasurementsInTemplate(String name)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        int resp = session.countMeasurementsInTemplate(name);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("countMeasurementsInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in countMeasurementsInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return -1;
  }

  @Override
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        boolean resp = session.isMeasurementInTemplate(templateName, path);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("isMeasurementInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in isMeasurementInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public boolean isPathExistInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        boolean resp = session.isPathExistInTemplate(templateName, path);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("isPathExistInTemplata failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in isPathExistInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public List<String> showMeasurementsInTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        List<String> resp = session.showMeasurementsInTemplate(templateName);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showMeasurementsInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in showMeasurementsInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public List<String> showMeasurementsInTemplate(String templateName, String pattern)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        List<String> resp = session.showMeasurementsInTemplate(templateName, pattern);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showMeasurementsInTemplate failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in showMeasurementsInTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public List<String> showAllTemplates()
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        List<String> resp = session.showAllTemplates();
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showAllTemplates failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in showAllTemplates", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public List<String> showPathsTemplateSetOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        List<String> resp = session.showPathsTemplateSetOn(templateName);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showPathsTemplateSetOn failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in showPathsTemplateSetOn", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public List<String> showPathsTemplateUsingOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        List<String> resp = session.showPathsTemplateUsingOn(templateName);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showPathsTemplateUsingOn failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in showPathsTemplateUsingOn", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public void sortTablet(Tablet tablet) throws IoTDBConnectionException {
    ISession session = getSession();
    session.sortTablet(tablet);
    putBack(session);
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.setSchemaTemplate(templateName, prefixPath);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("setSchemaTemplate [{}] on [{}] failed", templateName, prefixPath, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in setSchemaTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void unsetSchemaTemplate(String prefixPath, String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.unsetSchemaTemplate(prefixPath, templateName);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("unsetSchemaTemplate [{}] on [{}] failed", templateName, prefixPath, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in unsetSchemaTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void dropSchemaTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.dropSchemaTemplate(templateName);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("dropSchemaTemplate [{}] failed", templateName, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in dropSchemaTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.createTimeseriesUsingSchemaTemplate(devicePathList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("createTimeseriesOfSchemaTemplate {} failed", devicePathList, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in createTimeseriesUsingSchemaTemplate", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
   * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
   * more.
   *
   * @param sql query statement
   * @return result set Notice that you must get the result instance. Otherwise a data leakage will
   *     happen
   */
  @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
  @Override
  public SessionDataSetWrapper executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeQueryStatement(sql);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeQueryStatement failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in executeQueryStatement", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  /**
   * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
   * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
   * more.
   *
   * @param sql query statement
   * @param timeoutInMs the timeout of this query, in milliseconds
   * @return result set Notice that you must get the result instance. Otherwise a data leakage will
   *     happen
   */
  @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
  @Override
  public SessionDataSetWrapper executeQueryStatement(String sql, long timeoutInMs)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeQueryStatement(sql, timeoutInMs);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeQueryStatement failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in executeQueryStatement", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  @Override
  public void executeNonQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    ISession session = getSession();
    try {
      session.executeNonQueryStatement(sql);
      putBack(session);
    } catch (IoTDBConnectionException e) {
      // TException means the connection is broken, remove it and get a new one.
      LOGGER.warn("executeNonQueryStatement failed", e);
      cleanSessionAndMayThrowConnectionException(session, FINAL_RETRY, e);
    } catch (StatementExecutionException | RuntimeException e) {
      putBack(session);
      throw e;
    } catch (Throwable e) {
      LOGGER.error("unexpected error in executeNonQueryStatement", e);
      putBack(session);
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
  @Override
  public SessionDataSetWrapper executeRawDataQuery(
      List<String> paths, long startTime, long endTime, long timeOut)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeRawDataQuery(paths, startTime, endTime, timeOut);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeRawDataQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error("unexpected error in executeRawDataQuery", e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeLastDataQuery(List<String> paths, long lastTime)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeLastDataQuery(paths, lastTime);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_LASTDATAQUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_LASTDATAQUERY_ERROR, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeLastDataQuery(paths, lastTime, timeOut);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_LASTDATAQUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_LASTDATAQUERY_ERROR, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeLastDataQuery(paths);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_LASTDATAQUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_LASTDATAQUERY_ERROR, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp =
            session.executeLastDataQueryForOneDevice(db, device, sensors, isLegalPathNodes);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeLastDataQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_LASTDATAQUERY_ERROR, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp = session.executeAggregationQuery(paths, aggregations);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_AGGREGATION_QUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_AGGREGATION_QUERY_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp =
            session.executeAggregationQuery(paths, aggregations, startTime, endTime);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_AGGREGATION_QUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_AGGREGATION_QUERY_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp =
            session.executeAggregationQuery(paths, aggregations, startTime, endTime, interval);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_AGGREGATION_QUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_AGGREGATION_QUERY_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public SessionDataSetWrapper executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval,
      long slidingStep)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        SessionDataSet resp =
            session.executeAggregationQuery(
                paths, aggregations, startTime, endTime, interval, slidingStep);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn(EXECUTE_AGGREGATION_QUERY_FAIL, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      } catch (Throwable e) {
        LOGGER.error(EXECUTE_AGGREGATION_QUERY_ERROR_MSG, e);
        putBack(session);
        throw new RuntimeException(e);
      }
    }
    // never go here
    return null;
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    for (ISession session : queue) {
      session.setFetchSize(fetchSize);
    }
    for (ISession session : occupied.keySet()) {
      session.setFetchSize(fetchSize);
    }
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        session.setTimeZone(zoneId);
        putBack(session);
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("setTimeZone to [{}] failed", zoneId, e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    this.zoneId = ZoneId.of(zoneId);
    for (ISession session : queue) {
      session.setTimeZoneOfSession(zoneId);
    }
    for (ISession session : occupied.keySet()) {
      session.setTimeZoneOfSession(zoneId);
    }
  }

  @Override
  public ZoneId getZoneId() {
    return zoneId;
  }

  @Override
  public long getWaitToGetSessionTimeoutInMs() {
    return waitToGetSessionTimeoutInMs;
  }

  @Override
  public boolean isEnableCompression() {
    return enableCompression;
  }

  @Override
  public void setEnableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
    if (this.enableRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
    }
    for (ISession session : queue) {
      session.setEnableRedirection(enableRedirection);
    }
    for (ISession session : occupied.keySet()) {
      session.setEnableRedirection(enableRedirection);
    }
  }

  @Override
  public boolean isEnableRedirection() {
    return enableRedirection;
  }

  @Override
  public void setEnableQueryRedirection(boolean enableQueryRedirection) {
    this.enableQueryRedirection = enableQueryRedirection;
    for (ISession session : queue) {
      session.setEnableQueryRedirection(enableQueryRedirection);
    }
    for (ISession session : occupied.keySet()) {
      session.setEnableQueryRedirection(enableQueryRedirection);
    }
  }

  @Override
  public boolean isEnableQueryRedirection() {
    return enableQueryRedirection;
  }

  @Override
  public int getConnectionTimeoutInMs() {
    return connectionTimeoutInMs;
  }

  @Override
  public TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        TSBackupConfigurationResp resp = session.getBackupConfiguration();
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {

    for (int i = 0; i < RETRY; i++) {
      ISession session = getSession();
      try {
        TSConnectionInfoResp resp = session.fetchAllConnections();
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("fetchAllConnections failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (Throwable t) {
        putBack(session);
        throw t;
      }
    }
    return null;
  }

  @Override
  public void setVersion(Version version) {
    this.version = version;
    for (ISession session : queue) {
      session.setVersion(version);
    }
    for (ISession session : occupied.keySet()) {
      session.setVersion(version);
    }
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public void setQueryTimeout(long timeoutInMs) {
    this.queryTimeoutInMs = timeoutInMs;
    for (ISession session : queue) {
      session.setQueryTimeout(timeoutInMs);
    }
    for (ISession session : occupied.keySet()) {
      session.setQueryTimeout(timeoutInMs);
    }
  }

  @Override
  public long getQueryTimeout() {
    return queryTimeoutInMs;
  }

  public static class Builder {

    private String host = SessionConfig.DEFAULT_HOST;
    private int port = SessionConfig.DEFAULT_PORT;
    private List<String> nodeUrls = null;
    private int maxSize = SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE;
    private String user = SessionConfig.DEFAULT_USER;
    private String pw = SessionConfig.DEFAULT_PASSWORD;
    private int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
    private long waitToGetSessionTimeoutInMs = 60_000;
    private int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
    private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    private boolean enableCompression = false;
    private ZoneId zoneId = null;

    // this field only take effect in write request, nothing to do with any other type requests,
    // like query, load and so on.
    // if set to true, it means that we may redirect the write request to its corresponding leader
    // if set to false, it means that we will only send write request to first available DataNode(it
    // may be changed while current DataNode is not available, for example, we may retry to connect
    // to another available DataNode)
    // so even if enableRedirection is set to false, we may also send write request to another
    // datanode while encountering retriable errors in current DataNode
    private boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
    private boolean enableRecordsAutoConvertTablet =
        SessionConfig.DEFAULT_RECORDS_AUTO_CONVERT_TABLET;
    private int connectionTimeoutInMs = SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS;
    private Version version = SessionConfig.DEFAULT_VERSION;

    private boolean useSSL = false;
    private String trustStore;
    private String trustStorePwd;

    // set to true, means that we will start a background thread to fetch all available (Status is
    // not Removing) datanodes in cluster, and these available nodes will be used in retrying stage
    private boolean enableAutoFetch;

    // max retry count, if set to 0, means that we won't do any retry
    // we can use any available DataNodes(fetched in background thread if enableAutoFetch is true,
    // or nodeUrls user specified) to retry, even if enableRedirection is false
    private int maxRetryCount = SessionConfig.MAX_RETRY_COUNT;

    // sleep time between each retry
    private long retryIntervalInMs = SessionConfig.RETRY_INTERVAL_IN_MS;

    private long queryTimeoutInMs = SessionConfig.DEFAULT_QUERY_TIME_OUT;

    public Builder useSSL(boolean useSSL) {
      this.useSSL = useSSL;
      return this;
    }

    public Builder trustStore(String keyStore) {
      this.trustStore = keyStore;
      return this;
    }

    public Builder trustStorePwd(String keyStorePwd) {
      this.trustStorePwd = keyStorePwd;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder maxSize(int maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder password(String password) {
      this.pw = password;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder zoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
      this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
      return this;
    }

    public Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      return this;
    }

    public Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Builder enableCompression(boolean enableCompression) {
      this.enableCompression = enableCompression;
      return this;
    }

    public Builder enableRedirection(boolean enableRedirection) {
      this.enableRedirection = enableRedirection;
      return this;
    }

    public Builder enableRecordsAutoConvertTablet(boolean enableRecordsAutoConvertTablet) {
      this.enableRecordsAutoConvertTablet = enableRecordsAutoConvertTablet;
      return this;
    }

    public Builder connectionTimeoutInMs(int connectionTimeoutInMs) {
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      return this;
    }

    public Builder version(Version version) {
      this.version = version;
      return this;
    }

    public Builder enableAutoFetch(boolean enableAutoFetch) {
      this.enableAutoFetch = enableAutoFetch;
      return this;
    }

    public Builder maxRetryCount(int maxRetryCount) {
      this.maxRetryCount = maxRetryCount;
      return this;
    }

    public Builder retryIntervalInMs(long retryIntervalInMs) {
      this.retryIntervalInMs = retryIntervalInMs;
      return this;
    }

    public Builder queryTimeoutInMs(long queryTimeoutInMs) {
      this.queryTimeoutInMs = queryTimeoutInMs;
      return this;
    }

    public SessionPool build() {
      return new SessionPool(this);
    }
  }
}
