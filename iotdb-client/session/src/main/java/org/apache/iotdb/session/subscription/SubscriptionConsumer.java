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
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.util.SubscriptionPollTimer;
import org.apache.iotdb.session.util.SessionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class SubscriptionConsumer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumer.class);

  private static final long SLEEP_NS = 1_000_000_000L;

  private final String username;
  private final String password;

  protected String consumerId;
  protected String consumerGroupId;

  private final long heartbeatIntervalMs;
  private final long endpointsSyncIntervalMs;

  private SubscriptionProviders subscriptionProviders;

  private ScheduledExecutorService heartbeatWorkerExecutor;
  private ScheduledExecutorService endpointsSyncerExecutor;

  private ExecutorService asyncCommitExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  private final String tsFileBaseDir;

  private Path getTsFileDir(final String topicName) throws IOException {
    final Path dirPath =
        Paths.get(tsFileBaseDir).resolve(consumerGroupId).resolve(consumerId).resolve(topicName);
    Files.createDirectories(dirPath);
    return dirPath;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(final Builder builder) {
    final List<TEndPoint> initialEndpoints = new ArrayList<>();
    // From org.apache.iotdb.session.Session.getNodeUrls
    // Priority is given to `host:port` over `nodeUrls`.
    if (Objects.nonNull(builder.host)) {
      initialEndpoints.add(new TEndPoint(builder.host, builder.port));
    } else {
      initialEndpoints.addAll(SessionUtils.parseSeedNodeUrls(builder.nodeUrls));
    }
    this.subscriptionProviders = new SubscriptionProviders(initialEndpoints);

    this.username = builder.username;
    this.password = builder.password;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;

    this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
    this.endpointsSyncIntervalMs = builder.endpointsSyncIntervalMs;

    this.tsFileBaseDir = builder.tsFileBaseDir;
  }

  protected SubscriptionConsumer(final Builder builder, final Properties properties) {
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

  public synchronized void open() throws SubscriptionException, IoTDBConnectionException {
    if (!isClosed.get()) {
      return;
    }

    // open subscription providers
    subscriptionProviders.acquireWriteLock();
    try {
      subscriptionProviders.openProviders(
          this); // throw SubscriptionException or IoTDBConnectionException
    } finally {
      subscriptionProviders.releaseWriteLock();
    }

    // launch heartbeat worker
    launchHeartbeatWorker();

    // launch endpoints syncer
    launchEndpointsSyncer();

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws SubscriptionException, IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      // shutdown endpoints syncer
      shutdownEndpointsSyncer();

      // shutdown workers: heartbeat worker and async commit executor
      shutdownWorkers();

      // close subscription providers
      subscriptionProviders.acquireWriteLock();
      try {
        subscriptionProviders
            .closeProviders(); // throw SubscriptionException or IoTDBConnectionException
      } finally {
        subscriptionProviders.releaseWriteLock();
      }
    } finally {
      isClosed.set(true);
    }
  }

  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// subscribe & unsubscribe ///////////////////////////////

  public void subscribe(final String topicName) throws SubscriptionException {
    subscribe(Collections.singleton(topicName));
  }

  public void subscribe(final String... topicNames) throws SubscriptionException {
    subscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void subscribe(final Set<String> topicNames) throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      subscribeWithRedirection(topicNames);
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  public void unsubscribe(final String topicName) throws SubscriptionException {
    unsubscribe(Collections.singleton(topicName));
  }

  public void unsubscribe(final String... topicNames) throws SubscriptionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      unsubscribeWithRedirection(topicNames);
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchHeartbeatWorker() {
    heartbeatWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              final Thread t =
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
        () -> subscriptionProviders.heartbeat(this), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
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

  /////////////////////////////// sync endpoints ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchEndpointsSyncer() {
    endpointsSyncerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              final Thread t =
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
        () -> subscriptionProviders.sync(this), 0, endpointsSyncIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void shutdownEndpointsSyncer() {
    endpointsSyncerExecutor.shutdown();
    endpointsSyncerExecutor = null;
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  SubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
      throws SubscriptionException, IoTDBConnectionException {
    final SubscriptionProvider provider =
        new SubscriptionProvider(
            endPoint, this.username, this.password, this.consumerId, this.consumerGroupId);
    provider.handshake();

    // update consumer id and consumer group id if not exist
    if (Objects.isNull(this.consumerId)) {
      this.consumerId = provider.getConsumerId();
    }
    if (Objects.isNull(this.consumerGroupId)) {
      this.consumerGroupId = provider.getConsumerGroupId();
    }

    return provider;
  }

  /////////////////////////////// poll ///////////////////////////////

  protected List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs)
      throws SubscriptionException {
    final List<SubscriptionMessage> messages = new ArrayList<>();
    final SubscriptionPollTimer timer =
        new SubscriptionPollTimer(System.currentTimeMillis(), timeoutMs);

    do {
      // poll tablets or tsfile
      for (final SubscriptionPolledMessage polledMessage : pollInternal(topicNames)) {
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
                      ((TsFileInitMessagePayload) polledMessage.getMessagePayload()).getFileName())
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
      if (!messages.isEmpty()) {
        return messages;
      }
      // update timer
      timer.update();
      LockSupport.parkNanos(SLEEP_NS); // wait some time
    } while (timer.notExpired());

    LOGGER.info(
        "SubscriptionConsumer {} poll empty message after {} millisecond(s)", this, timeoutMs);
    return messages;
  }

  private Optional<SubscriptionMessage> pollTsFile(
      final SubscriptionCommitContext commitContext, String fileName) throws SubscriptionException {
    final String topicName = commitContext.getTopicName();
    Path filePath;

    try {
      filePath = getTsFileDir(topicName).resolve(fileName);
      Files.createFile(filePath);
    } catch (final FileAlreadyExistsException fileAlreadyExistsException) {
      LOGGER.info(
          "FileAlreadyExistsException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}, append \".1\" to file name",
          this,
          fileName,
          commitContext,
          fileAlreadyExistsException);
      fileName += ".1";
      try {
        filePath = getTsFileDir(topicName).resolve(fileName);
        Files.createFile(filePath);
      } catch (final IOException e) {
        LOGGER.warn(
            "IOException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}",
            this,
            fileName,
            commitContext,
            e);
        // TODO: Consider mid-process failures.
        // rethrow
        throw new SubscriptionNonRetryableException(e.getMessage(), e);
      }
    } catch (final IOException e) {
      LOGGER.warn(
          "IOException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}",
          this,
          fileName,
          commitContext,
          e);
      // TODO: Consider mid-process failures.
      // rethrow
      throw new SubscriptionNonRetryableException(e.getMessage(), e);
    }

    final File file = filePath.toFile();
    try (final RandomAccessFile fileWriter = new RandomAccessFile(file, "rw")) {
      return Optional.of(pollTsFileInternal(commitContext, file, fileWriter));
    } catch (final IOException | SubscriptionRetryableException e) {
      LOGGER.warn(
          "IOException or SubscriptionRetryableException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}",
          this,
          fileName,
          commitContext,
          e);
      return Optional.empty();
    } catch (final SubscriptionNonRetryableException e) {
      LOGGER.warn(
          "SubscriptionNonRetryableException occurred when SubscriptionConsumer {} polling TsFile {} with commit context {}",
          this,
          fileName,
          commitContext,
          e);
      // TODO: Consider mid-process failures.
      // rethrow
      throw e;
    }
  }

  private SubscriptionMessage pollTsFileInternal(
      final SubscriptionCommitContext commitContext,
      final File file,
      final RandomAccessFile fileWriter)
      throws IOException, SubscriptionException {
    final int dataNodeId = commitContext.getDataNodeId();
    final String topicName = commitContext.getTopicName();
    final String fileName = file.getName();

    LOGGER.info(
        "{} start to poll TsFile {} with commit context {}",
        this,
        file.getAbsolutePath(),
        commitContext);

    long writingOffset = fileWriter.length();
    while (true) {
      final List<SubscriptionPolledMessage> polledMessages =
          pollTsFileInternal(dataNodeId, topicName, fileName, writingOffset);

      // It's agreed that the server will always return at least one message, even in case of
      // failure.
      if (polledMessages.isEmpty()) {
        final String errorMessage =
            String.format("SubscriptionConsumer %s poll empty tsfile message", this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionNonRetryableException(errorMessage);
      }

      final SubscriptionPolledMessage polledMessage = polledMessages.get(0);
      final SubscriptionMessagePayload messagePayload = polledMessage.getMessagePayload();

      final short messageType = polledMessage.getMessageType();
      if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPolledMessageType.valueOf(messageType)) {
          case TS_FILE_PIECE:
            {
              // check commit context
              final SubscriptionCommitContext incomingCommitContext =
                  polledMessage.getCommitContext();
              if (Objects.isNull(incomingCommitContext)
                  || !Objects.equals(commitContext, incomingCommitContext)) {
                final String errorMessage =
                    String.format(
                        "inconsistent commit context, current is %s, incoming is %s, consumer: %s",
                        commitContext, incomingCommitContext, this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // check file name
              if (!fileName.startsWith(
                  ((TsFilePieceMessagePayload) messagePayload).getFileName())) {
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
              // check commit context
              final SubscriptionCommitContext incomingCommitContext =
                  polledMessage.getCommitContext();
              if (Objects.isNull(incomingCommitContext)
                  || !Objects.equals(commitContext, incomingCommitContext)) {
                final String errorMessage =
                    String.format(
                        "inconsistent commit context, current is %s, incoming is %s, consumer: %s",
                        commitContext, incomingCommitContext, this);
                LOGGER.warn(errorMessage);
                throw new SubscriptionNonRetryableException(errorMessage);
              }

              // check file name
              if (!fileName.startsWith(((TsFileSealMessagePayload) messagePayload).getFileName())) {
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
              // no need to check commit context

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

  private List<SubscriptionPolledMessage> pollInternal(final Set<String> topicNames)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getNextAvailableProvider();
      if (Objects.isNull(provider)) {
        return Collections.emptyList();
      }
      try {
        return provider.poll(
            new SubscriptionPollMessage(
                SubscriptionPollMessageType.POLL.getType(),
                new PollMessagePayload(topicNames),
                0L));
      } catch (final SubscriptionRetryableException e) {
        LOGGER.warn(
            "SubscriptionRetryableException occurred when SubscriptionConsumer {} polling from SubscriptionProvider {}",
            this,
            provider,
            e);
        // ignore
      } catch (final SubscriptionNonRetryableException e) {
        LOGGER.warn(
            "SubscriptionNonRetryableException occurred when SubscriptionConsumer {} polling from SubscriptionProvider {}",
            this,
            provider,
            e);
        // rethrow
        throw e;
      }
    } finally {
      subscriptionProviders.releaseReadLock();
    }

    return Collections.emptyList();
  }

  private List<SubscriptionPolledMessage> pollTsFileInternal(
      final int dataNodeId, final String topicName, final String fileName, final long writingOffset)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getProvider(dataNodeId);
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
              0L));
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  /////////////////////////////// commit sync ///////////////////////////////

  protected void commitSync(final Iterable<SubscriptionMessage> messages)
      throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionMessage message : messages) {
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(message.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(message.getCommitContext());
    }
    for (final Map.Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitSyncInternal(entry.getKey(), entry.getValue());
    }
  }

  private void commitSyncInternal(
      final int dataNodeId, final List<SubscriptionCommitContext> subscriptionCommitContexts)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when commit messages to subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                dataNodeId));
      }
      provider.commitSync(subscriptionCommitContexts);
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  /////////////////////////////// commit async ///////////////////////////////

  protected void commitAsync(final Iterable<SubscriptionMessage> messages) {
    commitAsync(messages, new AsyncCommitCallback() {});
  }

  protected void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    // Initiate executor if needed
    if (asyncCommitExecutor == null) {
      synchronized (this) {
        if (asyncCommitExecutor != null) {
          return;
        }

        asyncCommitExecutor =
            Executors.newSingleThreadExecutor(
                r -> {
                  final Thread t =
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

  private void subscribeWithRedirection(final Set<String> topicNames) throws SubscriptionException {
    final List<SubscriptionProvider> providers = subscriptionProviders.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers when %s subscribe topic %s",
              this, topicNames));
    }
    for (final SubscriptionProvider provider : providers) {
      try {
        provider.subscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to subscribe topics {} from subscription provider {}, try next subscription provider...",
            this,
            topicNames,
            provider,
            e);
      }
    }
    final String errorMessage =
        String.format(
            "%s failed to subscribe topics %s from all available subscription providers %s",
            this, topicNames, providers);
    LOGGER.warn(errorMessage);
    throw new SubscriptionNonRetryableException(errorMessage);
  }

  private void unsubscribeWithRedirection(final Set<String> topicNames)
      throws SubscriptionException {
    final List<SubscriptionProvider> providers = subscriptionProviders.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers when %s unsubscribe topic %s",
              this, topicNames));
    }
    for (final SubscriptionProvider provider : providers) {
      try {
        provider.unsubscribe(topicNames);
        return;
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to unsubscribe topics {} from subscription provider {}, try next subscription provider...",
            this,
            topicNames,
            provider,
            e);
      }
    }
    final String errorMessage =
        String.format(
            "%s failed to unsubscribe topics %s from all available subscription providers %s",
            this, topicNames, providers);
    LOGGER.warn(errorMessage);
    throw new SubscriptionNonRetryableException(errorMessage);
  }

  Map<Integer, TEndPoint> fetchAllEndPointsWithRedirection() throws SubscriptionException {
    final List<SubscriptionProvider> providers = subscriptionProviders.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers when %s fetch all endpoints", this));
    }
    for (final SubscriptionProvider provider : providers) {
      try {
        return provider.getSessionConnection().fetchAllEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            "{} failed to fetch all endpoints from subscription provider {}, try next subscription provider...",
            this,
            provider,
            e);
      }
    }
    final String errorMessage =
        String.format(
            "%s failed to fetch all endpoints from all available subscription providers %s",
            this, providers);
    LOGGER.warn(errorMessage);
    throw new SubscriptionNonRetryableException(errorMessage);
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

    public Builder host(final String host) {
      this.host = host;
      return this;
    }

    public Builder port(final int port) {
      this.port = port;
      return this;
    }

    public Builder nodeUrls(final List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder username(final String username) {
      this.username = username;
      return this;
    }

    public Builder password(final String password) {
      this.password = password;
      return this;
    }

    public Builder consumerId(final String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    public Builder consumerGroupId(final String consumerGroupId) {
      this.consumerGroupId = consumerGroupId;
      return this;
    }

    public Builder heartbeatIntervalMs(final long heartbeatIntervalMs) {
      this.heartbeatIntervalMs =
          Math.max(heartbeatIntervalMs, ConsumerConstant.HEARTBEAT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder endpointsSyncIntervalMs(final long endpointsSyncIntervalMs) {
      this.endpointsSyncIntervalMs =
          Math.max(endpointsSyncIntervalMs, ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder tsFileBaseDir(final String tsFileBaseDir) {
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

    public AsyncCommitWorker(
        final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
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
      } catch (final Exception e) {
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
