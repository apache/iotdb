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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionNonRetryableException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRetryableException;
import org.apache.iotdb.rpc.subscription.payload.common.PollMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.PollTsFileMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TabletsMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileErrorMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileInitMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFilePieceMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileSealMessagePayload;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public abstract class SubscriptionConsumer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumer.class);

  private static final IoTDBConnectionException NO_PROVIDERS_EXCEPTION =
      new IoTDBConnectionException("Cluster has no available subscription providers to connect");

  private final List<TEndPoint> initialEndpoints;

  private final String username;
  private final String password;

  protected final String consumerId;
  protected final String consumerGroupId;

  private final long heartbeatIntervalMs;
  private final long endpointsSyncIntervalMs;

  private final Map<Integer, SubscriptionProvider> subscriptionProviders =
      new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock subscriptionProvidersLock = new ReentrantReadWriteLock(true);

  private ScheduledExecutorService heartbeatWorkerExecutor;
  private ScheduledExecutorService endpointsSyncerExecutor;

  private ExecutorService asyncCommitExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  private final String tsFileBaseDir;

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// tsfile ///////////////////////////////

  private static final int POLL_TS_FILE_RETRY_LIMIT = 3;

  private final Map<String, SubscriptionTsFileInfo> topicNameToSubscriptionTsFileInfo =
      new ConcurrentHashMap<>();

  private static class SubscriptionTsFileInfo {

    SubscriptionCommitContext commitContext;
    File file;
    RandomAccessFile fileWriter;
    int retryCount;

    SubscriptionTsFileInfo(
        SubscriptionCommitContext commitContext, File file, RandomAccessFile fileWriter) {
      this.commitContext = commitContext;
      this.file = file;
      this.fileWriter = fileWriter;
      this.retryCount = 0;
    }

    String getTopicName() {
      return commitContext.getTopicName();
    }

    /** @return {@code true} if exceed retry limit */
    boolean increaseRetryCountAndCheckIfExceedRetryLimit() {
      retryCount++;
      return retryCount > POLL_TS_FILE_RETRY_LIMIT;
    }

    @Override
    public String toString() {
      return "SubscriptionTsFileInfo{"
          + "commitContext="
          + commitContext
          + ", file="
          + file.getAbsoluteFile()
          + ", retryCount="
          + retryCount
          + "}";
    }
  }

  private Path getTsFileDir(final String topicName) throws IOException {
    final Path dirPath =
        Paths.get(tsFileBaseDir).resolve(consumerGroupId).resolve(consumerId).resolve(topicName);
    Files.createDirectories(dirPath);
    return dirPath;
  }

  private SubscriptionTsFileInfo getSubscriptionTsFileInfoTsFileInfo(
      SubscriptionCommitContext commitContext, String fileName) throws IOException {
    final String topicName = commitContext.getTopicName();
    SubscriptionTsFileInfo info = topicNameToSubscriptionTsFileInfo.get(topicName);
    if (Objects.isNull(info)) {
      info = createSubscriptionTsFileInfoTsFileInfo(commitContext, fileName);
    } else {
      if (!info.file.exists()) {
        try {
          info.fileWriter.close();
        } catch (final IOException e) {
          LOGGER.warn(e.getMessage());
        }
        LOGGER.info(
            "consumer {} remove subscription TsFile info {} because the file does not exist",
            this,
            info);
        topicNameToSubscriptionTsFileInfo.remove(topicName);
        info = createSubscriptionTsFileInfoTsFileInfo(commitContext, fileName);
      }
    }

    return info;
  }

  private SubscriptionTsFileInfo createSubscriptionTsFileInfoTsFileInfo(
      SubscriptionCommitContext commitContext, String fileName) throws IOException {
    final String topicName = commitContext.getTopicName();
    final Path filePath = getTsFileDir(topicName).resolve(fileName);

    Files.createFile(filePath);
    final File file = filePath.toFile();
    final RandomAccessFile fileWriter = new RandomAccessFile(file, "rw");

    final SubscriptionTsFileInfo info = new SubscriptionTsFileInfo(commitContext, file, fileWriter);
    topicNameToSubscriptionTsFileInfo.put(topicName, info);
    LOGGER.info("consumer {} create subscription TsFile info {}", this, info);
    return info;
  }

  private void removeSubscriptionTsFileInfo(String topicName) {
    final SubscriptionTsFileInfo info = topicNameToSubscriptionTsFileInfo.get(topicName);
    if (Objects.isNull(info)) {
      return;
    }

    try {
      info.fileWriter.close();
    } catch (final IOException e) {
      LOGGER.warn(e.getMessage());
    }

    LOGGER.info("consumer {} remove subscription TsFile info {}", this, info);
    topicNameToSubscriptionTsFileInfo.remove(topicName);
  }

  private void increaseSubscriptionTsFileInfoRetryCountOrRemove(String topicName) {
    final SubscriptionTsFileInfo info = topicNameToSubscriptionTsFileInfo.get(topicName);
    if (Objects.isNull(info)) {
      return;
    }

    if (info.increaseRetryCountAndCheckIfExceedRetryLimit()) {
      removeSubscriptionTsFileInfo(topicName);
    }
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(Builder builder) {
    this.initialEndpoints = new ArrayList<>();
    // From org.apache.iotdb.session.Session.getNodeUrls
    // Priority is given to `host:port` over `nodeUrls`.
    if (Objects.nonNull(builder.host)) {
      initialEndpoints.add(new TEndPoint(builder.host, builder.port));
    } else {
      initialEndpoints.addAll(SessionUtils.parseSeedNodeUrls(builder.nodeUrls));
    }

    this.username = builder.username;
    this.password = builder.password;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;

    this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
    this.endpointsSyncIntervalMs = builder.endpointsSyncIntervalMs;

    this.tsFileBaseDir = builder.tsFileBaseDir;
  }

  protected SubscriptionConsumer(Builder builder, Properties properties) {
    this(
        builder
            .host(
                (String)
                    properties.getOrDefault(ConsumerConstant.HOST_KEY, SessionConfig.DEFAULT_HOST))
            .port(
                (Integer)
                    properties.getOrDefault(ConsumerConstant.PORT_KEY, SessionConfig.DEFAULT_PORT))
            .nodeUrls((List<String>) properties.get(ConsumerConstant.NODE_URLS_KEY))
            .username(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.USERNAME_KEY, SessionConfig.DEFAULT_USER))
            .password(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.PASSWORD_KEY, SessionConfig.DEFAULT_PASSWORD))
            .consumerId((String) properties.get(ConsumerConstant.CONSUMER_ID_KEY))
            .consumerGroupId((String) properties.get(ConsumerConstant.CONSUMER_GROUP_ID_KEY))
            .heartbeatIntervalMs(
                (Long)
                    properties.getOrDefault(
                        ConsumerConstant.HEARTBEAT_INTERVAL_MS_KEY,
                        ConsumerConstant.HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE))
            .endpointsSyncIntervalMs(
                (Long)
                    properties.getOrDefault(
                        ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_KEY,
                        ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE))
            .tsFileBaseDir(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.TS_FILE_BASE_DIR_KEY,
                        ConsumerConstant.TS_FILE_BASE_DIR_DEFAULT_VALUE)));
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    // open subscription providers
    acquireWriteLock();
    try {
      openProviders(); // throw IoTDBConnectionException
    } finally {
      releaseWriteLock();
    }

    // launch heartbeat worker
    launchHeartbeatWorker();

    // launch endpoints syncer
    launchEndpointsSyncer();

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      // shutdown endpoints syncer
      shutdownEndpointsSyncer();

      // shutdown workers: heartbeat worker and async commit executor
      shutdownWorkers();

      // close subscription providers
      acquireWriteLock();
      try {
        closeProviders();
      } finally {
        releaseWriteLock();
      }
    } finally {
      isClosed.set(true);
    }
  }

  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// lock ///////////////////////////////

  void acquireReadLock() {
    subscriptionProvidersLock.readLock().lock();
  }

  void releaseReadLock() {
    subscriptionProvidersLock.readLock().unlock();
  }

  void acquireWriteLock() {
    subscriptionProvidersLock.writeLock().lock();
  }

  void releaseWriteLock() {
    subscriptionProvidersLock.writeLock().unlock();
  }

  /////////////////////////////// subscribe & unsubscribe ///////////////////////////////

  public void subscribe(String topicName)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    subscribe(Collections.singleton(topicName));
  }

  public void subscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    subscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void subscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      subscribeWithRedirection(topicNames);
    } finally {
      releaseReadLock();
    }
  }

  public void unsubscribe(String topicName)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    unsubscribe(Collections.singleton(topicName));
  }

  public void unsubscribe(String... topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void unsubscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      unsubscribeWithRedirection(topicNames);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchHeartbeatWorker() {
    heartbeatWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "ConsumerHeartbeatWorker", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    heartbeatWorkerExecutor.scheduleAtFixedRate(
        new ConsumerHeartbeatWorker(this), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Shut down workers upon close. There are currently two workers: heartbeat worker and async
   * commit executor.
   */
  private void shutdownWorkers() {
    heartbeatWorkerExecutor.shutdown();
    heartbeatWorkerExecutor = null;

    if (asyncCommitExecutor != null) {
      asyncCommitExecutor.shutdown();
      asyncCommitExecutor = null;
    }
  }

  /////////////////////////////// endpoints syncer ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchEndpointsSyncer() {
    endpointsSyncerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(), r, "SubscriptionEndpointsSyncer", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    endpointsSyncerExecutor.scheduleAtFixedRate(
        new SubscriptionEndpointsSyncer(this), 0, endpointsSyncIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void shutdownEndpointsSyncer() {
    endpointsSyncerExecutor.shutdown();
    endpointsSyncerExecutor = null;
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  SubscriptionProvider constructProvider(final TEndPoint endPoint) {
    return new SubscriptionProvider(
        endPoint, this.username, this.password, this.consumerId, this.consumerGroupId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void openProviders() throws IoTDBConnectionException {
    // close stale providers
    closeProviders();

    for (final TEndPoint endPoint : initialEndpoints) {
      final SubscriptionProvider defaultProvider;
      final int defaultDataNodeId;

      try {
        defaultProvider = constructProvider(endPoint);
        defaultDataNodeId = defaultProvider.handshake();
      } catch (final Exception e) {
        LOGGER.warn("Failed to create connection with {}, exception: {}", endPoint, e.getMessage());
        continue; // try next endpoint
      }
      addProvider(defaultDataNodeId, defaultProvider);

      final Map<Integer, TEndPoint> allEndPoints;
      try {
        allEndPoints = defaultProvider.getSessionConnection().fetchAllEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to fetch all endpoints from {}, exception: {}, will retry later...",
            endPoint,
            e.getMessage());
        break; // retry later
      }

      for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
        if (defaultDataNodeId == entry.getKey()) {
          continue;
        }

        final SubscriptionProvider provider;
        try {
          provider = constructProvider(entry.getValue());
          provider.handshake();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with {}, exception: {}, will retry later...",
              entry.getValue(),
              e.getMessage());
          continue; // retry later
        }
        addProvider(entry.getKey(), provider);
      }

      break;
    }

    if (hasNoProviders()) {
      throw NO_PROVIDERS_EXCEPTION;
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  private void closeProviders() throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAllProviders()) {
      provider.close();
    }
    subscriptionProviders.clear();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void addProvider(final int dataNodeId, final SubscriptionProvider provider) {
    // the subscription provider is opened
    LOGGER.info("add new subscription provider {}", provider);
    subscriptionProviders.put(dataNodeId, provider);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireWriteLock()}. */
  void closeAndRemoveProvider(final int dataNodeId) throws IoTDBConnectionException {
    if (!containsProvider(dataNodeId)) {
      return;
    }
    final SubscriptionProvider provider = subscriptionProviders.get(dataNodeId);
    try {
      provider.close();
    } finally {
      LOGGER.info("close and remove stale subscription provider {}", provider);
      subscriptionProviders.remove(dataNodeId);
    }
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean hasNoProviders() {
    return subscriptionProviders.isEmpty();
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  boolean containsProvider(final int dataNodeId) {
    return subscriptionProviders.containsKey(dataNodeId);
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAllAvailableProviders() {
    return subscriptionProviders.values().stream()
        .filter(SubscriptionProvider::isAvailable)
        .collect(Collectors.toList());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  List<SubscriptionProvider> getAllProviders() {
    return new ArrayList<>(subscriptionProviders.values());
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  SubscriptionProvider getProvider(final int dataNodeId) {
    return containsProvider(dataNodeId) ? subscriptionProviders.get(dataNodeId) : null;
  }

  /////////////////////////////// poll ///////////////////////////////

  protected List<SubscriptionMessage> poll(Set<String> topicNames, long timeoutMs)
      throws SubscriptionException {
    final List<SubscriptionMessage> messages = new ArrayList<>();

    // poll on the fly tsfile
    for (final SubscriptionTsFileInfo info :
        topicNameToSubscriptionTsFileInfo.values().stream()
            .filter(
                info -> {
                  if (topicNames.isEmpty()) {
                    return true;
                  }
                  return topicNames.contains(info.getTopicName());
                })
            .collect(Collectors.toList())) {
      pollTsFile(info.commitContext, info.file.getName(), timeoutMs).ifPresent(messages::add);
    }

    // poll tablets or tsfile
    for (final SubscriptionPolledMessage polledMessage : pollInternal(topicNames, timeoutMs)) {
      final short messageType = polledMessage.getMessageType();
      if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPolledMessageType.valueOf(messageType)) {
          case TABLETS:
            messages.add(
                new SubscriptionMessage(
                    polledMessage.getCommitContext(),
                    ((TabletsMessagePayload) polledMessage.getMessagePayload()).getTablets()));
            break;
          case TS_FILE_INIT:
            pollTsFile(
                    polledMessage.getCommitContext(),
                    ((TsFileInitMessagePayload) polledMessage.getMessagePayload()).getFileName(),
                    timeoutMs)
                .ifPresent(messages::add);
            break;
          default:
            LOGGER.warn("unexpected message type: {}", messageType);
            break;
        }
      } else {
        LOGGER.warn("unexpected message type: {}", messageType);
      }
    }

    return messages;
  }

  private Optional<SubscriptionMessage> pollTsFile(
      SubscriptionCommitContext commitContext, String fileName, long timeoutMs)
      throws SubscriptionException {
    try {
      final SubscriptionMessage message = pollTsFileInternal(commitContext, fileName, timeoutMs);
      removeSubscriptionTsFileInfo(commitContext.getTopicName());
      return Optional.of(message);
    } catch (IOException | SubscriptionRetryableException e) {
      LOGGER.warn(
          "IOException or SubscriptionRetryableException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}: {}",
          this,
          fileName,
          commitContext,
          e.getMessage());
      // assume retryable
      increaseSubscriptionTsFileInfoRetryCountOrRemove(commitContext.getTopicName());
    } catch (SubscriptionNonRetryableException e) {
      LOGGER.warn(
          "SubscriptionNonRetryableException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}: {}",
          this,
          fileName,
          commitContext,
          e.getMessage());
      // assume non-retryable
      removeSubscriptionTsFileInfo(commitContext.getTopicName());
      throw e;
    }
    return Optional.empty();
  }

  private SubscriptionMessage pollTsFileInternal(
      SubscriptionCommitContext commitContext, String fileName, long timeoutMs)
      throws IOException, SubscriptionException {
    final int dataNodeId = commitContext.getDataNodeId();
    final String topicName = commitContext.getTopicName();

    final SubscriptionTsFileInfo info =
        getSubscriptionTsFileInfoTsFileInfo(commitContext, fileName);
    final File file = info.file;
    final RandomAccessFile fileWriter = info.fileWriter;

    LOGGER.info(
        "{} start to poll TsFile {} with commit context {}",
        this,
        file.getAbsolutePath(),
        commitContext);

    long writingOffset = fileWriter.length();
    while (true) {
      final List<SubscriptionPolledMessage> polledMessages =
          pollTsFileInternal(dataNodeId, topicName, fileName, writingOffset, timeoutMs);

      if (polledMessages.isEmpty()) {
        final String errorMessage =
            String.format("SubscriptionConsumer %s poll empty tsfile message", this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionNonRetryableException(errorMessage);
      }

      final SubscriptionPolledMessage polledMessage = polledMessages.get(0);
      final SubscriptionMessagePayload messagePayload = polledMessage.getMessagePayload();
      final SubscriptionCommitContext incomingCommitContext = polledMessage.getCommitContext();
      if (Objects.isNull(incomingCommitContext)
          || !Objects.equals(commitContext, incomingCommitContext)) {
        final String errorMessage =
            String.format(
                "inconsistent commit context, current is %s, incoming is %s, consumer: %s",
                commitContext, incomingCommitContext, this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionNonRetryableException(errorMessage);
      }

      final short messageType = polledMessage.getMessageType();
      if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPolledMessageType.valueOf(messageType)) {
          case TS_FILE_PIECE:
            {
              // check file name
              if (!Objects.equals(
                  fileName, ((TsFilePieceMessagePayload) messagePayload).getFileName())) {
                final String errorMessage =
                    String.format(
                        "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                        fileName, ((TsFilePieceMessagePayload) messagePayload).getFileName(), this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // write file piece
              fileWriter.write(((TsFilePieceMessagePayload) messagePayload).getFilePiece());
              fileWriter.getFD().sync();

              // check offset
              if (!Objects.equals(
                  fileWriter.length(),
                  ((TsFilePieceMessagePayload) messagePayload).getNextWritingOffset())) {
                final String errorMessage =
                    String.format(
                        "inconsistent file offset, current is %s, incoming is %s, consumer: %s",
                        fileWriter.length(),
                        ((TsFilePieceMessagePayload) messagePayload).getNextWritingOffset(),
                        this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // update offset
              writingOffset = ((TsFilePieceMessagePayload) messagePayload).getNextWritingOffset();
              break;
            }
          case TS_FILE_SEAL:
            {
              // check file name
              if (!Objects.equals(
                  fileName, ((TsFileSealMessagePayload) messagePayload).getFileName())) {
                final String errorMessage =
                    String.format(
                        "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                        fileName, ((TsFileSealMessagePayload) messagePayload).getFileName(), this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // check file length
              if (fileWriter.length()
                  != ((TsFileSealMessagePayload) messagePayload).getFileLength()) {
                final String errorMessage =
                    String.format(
                        "inconsistent file length, current is %s, incoming is %s, consumer: %s",
                        fileWriter.length(),
                        ((TsFileSealMessagePayload) messagePayload).getFileLength(),
                        this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // sync and close
              fileWriter.getFD().sync();
              fileWriter.close();

              LOGGER.info(
                  "SubscriptionConsumer {} successfully poll TsFile {} with commit context {}",
                  this,
                  file.getAbsolutePath(),
                  commitContext);

              // generate subscription message
              return new SubscriptionMessage(commitContext, file.getAbsolutePath());
            }
          case TS_FILE_ERROR:
            {
              final String errorMessage =
                  ((TsFileErrorMessagePayload) messagePayload).getErrorMessage();
              final boolean retryable = ((TsFileErrorMessagePayload) messagePayload).isRetryable();
              LOGGER.warn(
                  "Error occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}: {}, retryable: {}",
                  this,
                  file.getAbsolutePath(),
                  commitContext,
                  errorMessage,
                  retryable);
              if (retryable) {
                throw new SubscriptionRetryableException(errorMessage);
              } else {
                throw new SubscriptionNonRetryableException(errorMessage);
              }
            }
          default:
            final String errorMessage = String.format("unexpected message type: %s", messageType);
            LOGGER.warn(errorMessage);
            throw new SubscriptionNonRetryableException(errorMessage);
        }
      } else {
        final String errorMessage = String.format("unexpected message type: %s", messageType);
        LOGGER.warn(errorMessage);
        throw new SubscriptionNonRetryableException(errorMessage);
      }
    }
  }

  private List<SubscriptionPolledMessage> pollInternal(Set<String> topicNames, long timeoutMs)
      throws SubscriptionException {
    final List<SubscriptionPolledMessage> polledMessages = new ArrayList<>();

    acquireReadLock();
    try {
      for (final SubscriptionProvider provider : getAllAvailableProviders()) {
        try {
          polledMessages.addAll(
              provider.poll(
                  new SubscriptionPollMessage(
                      SubscriptionPollMessageType.POLL.getType(),
                      new PollMessagePayload(topicNames),
                      timeoutMs)));
        } catch (final Exception e) {
          LOGGER.warn(e.getMessage());
        }
      }
    } finally {
      releaseReadLock();
    }

    return polledMessages;
  }

  private List<SubscriptionPolledMessage> pollTsFileInternal(
      int dataNodeId, String topicName, String fileName, long writingOffset, long timeoutMs)
      throws SubscriptionException {
    acquireReadLock();
    try {
      final SubscriptionProvider provider = getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when poll TsFile from subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                dataNodeId));
      }
      return provider.poll(
          new SubscriptionPollMessage(
              SubscriptionPollMessageType.POLL_TS_FILE.getType(),
              new PollTsFileMessagePayload(topicName, fileName, writingOffset),
              timeoutMs));
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// commit sync ///////////////////////////////

  protected void commitSync(Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (SubscriptionMessage message : messages) {
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(message.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(message.getCommitContext());
    }
    for (Map.Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitSyncInternal(entry.getKey(), entry.getValue());
    }
  }

  private void commitSyncInternal(
      int dataNodeId, List<SubscriptionCommitContext> subscriptionCommitContexts)
      throws SubscriptionException {
    acquireReadLock();
    try {
      final SubscriptionProvider provider = getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when commit messages to subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                dataNodeId));
      }
      provider.commitSync(subscriptionCommitContexts);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// commit async ///////////////////////////////

  protected void commitAsync(Iterable<SubscriptionMessage> messages) {
    commitAsync(messages, new AsyncCommitCallback() {});
  }

  protected void commitAsync(Iterable<SubscriptionMessage> messages, AsyncCommitCallback callback) {
    // Initiate executor if needed
    if (asyncCommitExecutor == null) {
      synchronized (this) {
        if (asyncCommitExecutor != null) {
          return;
        }

        asyncCommitExecutor =
            Executors.newSingleThreadExecutor(
                r -> {
                  Thread t =
                      new Thread(
                          Thread.currentThread().getThreadGroup(),
                          r,
                          "SubscriptionConsumerAsyncCommitWorker",
                          0);
                  if (!t.isDaemon()) {
                    t.setDaemon(true);
                  }
                  if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                  }
                  return t;
                });
      }
    }

    asyncCommitExecutor.submit(new AsyncCommitWorker(messages, callback));
  }

  /////////////////////////////// redirection ///////////////////////////////

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  private void subscribeWithRedirection(final Set<String> topicNames)
      throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAllAvailableProviders()) {
      try {
        provider.getSessionConnection().subscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to subscribe topics {} from subscription provider {}, exception: {}, try next subscription provider...",
            topicNames,
            provider,
            e.getMessage());
      }
    }
    throw NO_PROVIDERS_EXCEPTION;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  private void unsubscribeWithRedirection(final Set<String> topicNames)
      throws IoTDBConnectionException {
    for (final SubscriptionProvider provider : getAllAvailableProviders()) {
      try {
        provider.getSessionConnection().unsubscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to unsubscribe topics {} from subscription provider {}, exception: {}, try next subscription provider...",
            topicNames,
            provider,
            e.getMessage());
      }
    }
    throw NO_PROVIDERS_EXCEPTION;
  }

  /** Caller should ensure that the method is called in the lock {@link #acquireReadLock()}. */
  Map<Integer, TEndPoint> fetchAllEndPointsWithRedirection() throws IoTDBConnectionException {
    Map<Integer, TEndPoint> endPoints = null;
    for (final SubscriptionProvider provider : getAllAvailableProviders()) {
      try {
        endPoints = provider.getSessionConnection().fetchAllEndPoints();
        break;
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to fetch all endpoints from subscription provider {}, exception: {}, try next subscription provider...",
            provider,
            e.getMessage());
      }
    }
    if (Objects.isNull(endPoints)) {
      throw NO_PROVIDERS_EXCEPTION;
    }
    return endPoints;
  }

  /////////////////////////////// builder ///////////////////////////////

  public abstract static class Builder {

    protected String host = SessionConfig.DEFAULT_HOST;
    protected int port = SessionConfig.DEFAULT_PORT;
    protected List<String> nodeUrls = null;

    protected String username = SessionConfig.DEFAULT_USER;
    protected String password = SessionConfig.DEFAULT_PASSWORD;

    protected String consumerId;
    protected String consumerGroupId;

    protected long heartbeatIntervalMs = ConsumerConstant.HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE;
    protected long endpointsSyncIntervalMs =
        ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE;

    protected String tsFileBaseDir = ConsumerConstant.TS_FILE_BASE_DIR_DEFAULT_VALUE;

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

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public Builder consumerGroupId(String consumerGroupId) {
      this.consumerGroupId = consumerGroupId;
      return this;
    }

    public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
      this.heartbeatIntervalMs =
          Math.max(heartbeatIntervalMs, ConsumerConstant.HEARTBEAT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder endpointsSyncIntervalMs(long endpointsSyncIntervalMs) {
      this.endpointsSyncIntervalMs =
          Math.max(endpointsSyncIntervalMs, ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder tsFileBaseDir(String tsFileBaseDir) {
      this.tsFileBaseDir = tsFileBaseDir;
      return this;
    }

    public abstract SubscriptionPullConsumer buildPullConsumer();

    public abstract SubscriptionPushConsumer buildPushConsumer();
  }

  /////////////////////////////// commit async worker ///////////////////////////////

  class AsyncCommitWorker implements Runnable {
    private final Iterable<SubscriptionMessage> messages;
    private final AsyncCommitCallback callback;

    public AsyncCommitWorker(Iterable<SubscriptionMessage> messages, AsyncCommitCallback callback) {
      this.messages = messages;
      this.callback = callback;
    }

    @Override
    public void run() {
      if (isClosed()) {
        return;
      }

      try {
        commitSync(messages);
        callback.onComplete();
      } catch (Exception e) {
        callback.onFailure(e);
      }
    }
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionConsumer{consumerId="
        + consumerId
        + ", consumerGroupId="
        + consumerGroupId
        + "}";
  }
}
