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
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollFilePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequest;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequestType;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.util.RandomStringGenerator;
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
import java.util.concurrent.CompletableFuture;
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

  private final SubscriptionProviders subscriptionProviders;

  private ScheduledExecutorService heartbeatWorkerExecutor;
  private ScheduledExecutorService endpointsSyncerExecutor;

  private ExecutorService asyncCommitExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  private final String fileSaveDir;

  private Path getFileDir(final String topicName) throws IOException {
    final Path dirPath =
        Paths.get(fileSaveDir).resolve(consumerGroupId).resolve(consumerId).resolve(topicName);
    Files.createDirectories(dirPath);
    return dirPath;
  }

  private Path getFilePath(final String topicName, String fileName) throws SubscriptionException {
    Path filePath;
    try {
      filePath = getFileDir(topicName).resolve(fileName);
      Files.createFile(filePath);
    } catch (final FileAlreadyExistsException fileAlreadyExistsException) {
      fileName += "." + RandomStringGenerator.generate(16);
      try {
        filePath = getFileDir(topicName).resolve(fileName);
        Files.createFile(filePath);
      } catch (final IOException e) {
        throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
      }
    } catch (final IOException e) {
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
    return filePath;
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

    this.fileSaveDir = builder.fileSaveDir;
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
            .fileSaveDir(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.FILE_SAVE_DIR_KEY,
                        ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE)));
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open() throws SubscriptionException {
    if (!isClosed.get()) {
      return;
    }

    // open subscription providers
    subscriptionProviders.acquireWriteLock();
    try {
      subscriptionProviders.openProviders(this); // throw SubscriptionException
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
  public synchronized void close() {
    if (isClosed.get()) {
      return;
    }

    try {
      // shutdown endpoints syncer
      shutdownEndpointsSyncer();

      // shutdown heartbeat worker
      shutdownHeartbeatWorker();

      // shutdown async commit worker if needed
      shutdownAsyncCommitWorkerIfNeeded();

      // close subscription providers
      subscriptionProviders.acquireWriteLock();
      try {
        subscriptionProviders.closeProviders();
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
    heartbeatWorkerExecutor.scheduleWithFixedDelay(
        () -> subscriptionProviders.heartbeat(this),
        generateRandomInitialDelayMs(heartbeatIntervalMs),
        heartbeatIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  private void shutdownHeartbeatWorker() {
    heartbeatWorkerExecutor.shutdown();
    heartbeatWorkerExecutor = null;
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
    endpointsSyncerExecutor.scheduleWithFixedDelay(
        () -> subscriptionProviders.sync(this),
        generateRandomInitialDelayMs(endpointsSyncIntervalMs),
        endpointsSyncIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  private void shutdownEndpointsSyncer() {
    endpointsSyncerExecutor.shutdown();
    endpointsSyncerExecutor = null;
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  SubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
      throws SubscriptionException {
    final SubscriptionProvider provider =
        new SubscriptionProvider(
            endPoint, this.username, this.password, this.consumerId, this.consumerGroupId);
    try {
      provider.handshake();
    } catch (final Exception e) {
      try {
        provider.close();
      } catch (final Exception ignored) {
      }
      throw new SubscriptionConnectionException(
          String.format("Failed to handshake with subscription provider %s", provider));
    }

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
      try {
        // poll tablets or file
        for (final SubscriptionPollResponse pollResponse : pollInternal(topicNames)) {
          final short responseType = pollResponse.getResponseType();
          if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
            LOGGER.warn("unexpected response type: {}", responseType);
            continue;
          }
          switch (SubscriptionPollResponseType.valueOf(responseType)) {
            case TABLETS:
              messages.add(
                  new SubscriptionMessage(
                      pollResponse.getCommitContext(),
                      ((TabletsPayload) pollResponse.getPayload()).getTablets()));
              break;
            case FILE_INIT:
              pollFile(
                      pollResponse.getCommitContext(),
                      ((FileInitPayload) pollResponse.getPayload()).getFileName())
                  .ifPresent(messages::add);
              break;
            case ERROR:
              final ErrorPayload payload = (ErrorPayload) pollResponse.getPayload();
              final String errorMessage = payload.getErrorMessage();
              final boolean critical = payload.isCritical();
              LOGGER.warn(
                  "Error occurred when SubscriptionConsumer {} polling topics {}: {}, critical: {}",
                  this,
                  topicNames,
                  errorMessage,
                  critical);
              if (critical) {
                throw new SubscriptionRuntimeCriticalException(errorMessage);
              } else {
                throw new SubscriptionRuntimeNonCriticalException(errorMessage);
              }
            default:
              LOGGER.warn("unexpected response type: {}", responseType);
              break;
          }
        }
      } catch (final SubscriptionRuntimeNonCriticalException e) {
        LOGGER.warn(
            "SubscriptionRuntimeNonCriticalException occurred when SubscriptionConsumer {} polling topics {}",
            this,
            topicNames,
            e);
        // nack and clear messages
        try {
          nack(messages);
          messages.clear();
        } catch (final Exception ignored) {
        }
      } catch (final SubscriptionRuntimeCriticalException e) {
        LOGGER.warn(
            "SubscriptionRuntimeCriticalException occurred when SubscriptionConsumer {} polling topics {}",
            this,
            topicNames,
            e);
        // nack and clear messages
        try {
          nack(messages);
          messages.clear();
        } catch (final Exception ignored) {
        }
        // rethrow
        throw e;
      }
      if (!messages.isEmpty()) {
        return messages;
      }
      // update timer
      timer.update();
      // TODO: associated with timeoutMs instead of hardcoding
      LockSupport.parkNanos(SLEEP_NS); // wait some time
    } while (timer.notExpired());

    LOGGER.info(
        "SubscriptionConsumer {} poll empty message after {} millisecond(s)", this, timeoutMs);
    return messages;
  }

  private Optional<SubscriptionMessage> pollFile(
      final SubscriptionCommitContext commitContext, final String fileName)
      throws SubscriptionException {
    final String topicName = commitContext.getTopicName();
    final Path filePath = getFilePath(topicName, fileName);
    final File file = filePath.toFile();
    try (final RandomAccessFile fileWriter = new RandomAccessFile(file, "rw")) {
      return Optional.of(pollFileInternal(commitContext, file, fileWriter));
    } catch (final IOException e) {
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  private SubscriptionMessage pollFileInternal(
      final SubscriptionCommitContext commitContext,
      final File file,
      final RandomAccessFile fileWriter)
      throws IOException, SubscriptionException {
    final int dataNodeId = commitContext.getDataNodeId();
    final String topicName = commitContext.getTopicName();
    final String fileName = file.getName();

    LOGGER.info(
        "{} start to poll file {} with commit context {}",
        this,
        file.getAbsolutePath(),
        commitContext);

    long writingOffset = fileWriter.length();
    while (true) {
      final List<SubscriptionPollResponse> responses =
          pollFileInternal(dataNodeId, topicName, fileName, writingOffset);

      // It's agreed that the server will always return at least one response, even in case of
      // failure.
      if (responses.isEmpty()) {
        final String errorMessage =
            String.format("SubscriptionConsumer %s poll empty response", this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      // Only one SubscriptionEvent polled currently...
      final SubscriptionPollResponse response = responses.get(0);
      final SubscriptionPollPayload payload = response.getPayload();
      final short responseType = response.getResponseType();
      if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
        final String errorMessage = String.format("unexpected response type: %s", responseType);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      switch (SubscriptionPollResponseType.valueOf(responseType)) {
        case FILE_PIECE:
          {
            // check commit context
            final SubscriptionCommitContext incomingCommitContext = response.getCommitContext();
            if (Objects.isNull(incomingCommitContext)
                || !Objects.equals(commitContext, incomingCommitContext)) {
              final String errorMessage =
                  String.format(
                      "inconsistent commit context, current is %s, incoming is %s, consumer: %s",
                      commitContext, incomingCommitContext, this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // check file name
            if (!fileName.startsWith(((FilePiecePayload) payload).getFileName())) {
              final String errorMessage =
                  String.format(
                      "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                      fileName, ((FilePiecePayload) payload).getFileName(), this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // write file piece
            fileWriter.write(((FilePiecePayload) payload).getFilePiece());
            fileWriter.getFD().sync();

            // check offset
            if (!Objects.equals(
                fileWriter.length(), ((FilePiecePayload) payload).getNextWritingOffset())) {
              final String errorMessage =
                  String.format(
                      "inconsistent file offset, current is %s, incoming is %s, consumer: %s",
                      fileWriter.length(),
                      ((FilePiecePayload) payload).getNextWritingOffset(),
                      this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // update offset
            writingOffset = ((FilePiecePayload) payload).getNextWritingOffset();
            break;
          }
        case FILE_SEAL:
          {
            // check commit context
            final SubscriptionCommitContext incomingCommitContext = response.getCommitContext();
            if (Objects.isNull(incomingCommitContext)
                || !Objects.equals(commitContext, incomingCommitContext)) {
              final String errorMessage =
                  String.format(
                      "inconsistent commit context, current is %s, incoming is %s, consumer: %s",
                      commitContext, incomingCommitContext, this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // check file name
            if (!fileName.startsWith(((FileSealPayload) payload).getFileName())) {
              final String errorMessage =
                  String.format(
                      "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                      fileName, ((FileSealPayload) payload).getFileName(), this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // check file length
            if (fileWriter.length() != ((FileSealPayload) payload).getFileLength()) {
              final String errorMessage =
                  String.format(
                      "inconsistent file length, current is %s, incoming is %s, consumer: %s",
                      fileWriter.length(), ((FileSealPayload) payload).getFileLength(), this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // sync and close
            fileWriter.getFD().sync();
            fileWriter.close();

            LOGGER.info(
                "SubscriptionConsumer {} successfully poll file {} with commit context {}",
                this,
                file.getAbsolutePath(),
                commitContext);

            // generate subscription message
            return new SubscriptionMessage(commitContext, file.getAbsolutePath());
          }
        case ERROR:
          {
            // no need to check commit context

            final String errorMessage = ((ErrorPayload) payload).getErrorMessage();
            final boolean critical = ((ErrorPayload) payload).isCritical();
            LOGGER.warn(
                "Error occurred when SubscriptionConsumer {} polling file {} with commit context {}: {}, critical: {}",
                this,
                file.getAbsolutePath(),
                commitContext,
                errorMessage,
                critical);
            if (critical) {
              throw new SubscriptionRuntimeCriticalException(errorMessage);
            } else {
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }
          }
        default:
          final String errorMessage = String.format("unexpected response type: %s", responseType);
          LOGGER.warn(errorMessage);
          throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }
    }
  }

  private List<SubscriptionPollResponse> pollInternal(final Set<String> topicNames)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getNextAvailableProvider();
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "Cluster has no available subscription providers when %s poll topic %s",
                this, topicNames));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.poll(
            new SubscriptionPollRequest(
                SubscriptionPollRequestType.POLL.getType(), new PollPayload(topicNames), 0L));
      } catch (final SubscriptionConnectionException ignored) {
        return Collections.emptyList();
      }
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  private List<SubscriptionPollResponse> pollFileInternal(
      final int dataNodeId, final String topicName, final String fileName, final long writingOffset)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when %s poll file from subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                this, dataNodeId));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.poll(
            new SubscriptionPollRequest(
                SubscriptionPollRequestType.POLL_FILE.getType(),
                new PollFilePayload(topicName, fileName, writingOffset),
                0L));
      } catch (final SubscriptionConnectionException ignored) {
        return Collections.emptyList();
      }
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  /////////////////////////////// commit sync (ack & nack) ///////////////////////////////

  protected void ack(final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionMessage message : messages) {
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(message.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(message.getCommitContext());
    }
    for (final Map.Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitInternal(entry.getKey(), entry.getValue(), false);
    }
  }

  protected void nack(final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionMessage message : messages) {
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(message.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(message.getCommitContext());
    }
    for (final Map.Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitInternal(entry.getKey(), entry.getValue(), true);
    }
  }

  private void commitInternal(
      final int dataNodeId,
      final List<SubscriptionCommitContext> subscriptionCommitContexts,
      final boolean nack)
      throws SubscriptionException {
    subscriptionProviders.acquireReadLock();
    try {
      final SubscriptionProvider provider = subscriptionProviders.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when %s commit (nack: %s) messages to subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                this, nack, dataNodeId));
      }
      provider.commit(subscriptionCommitContexts, nack);
    } finally {
      subscriptionProviders.releaseReadLock();
    }
  }

  /////////////////////////////// commit async ///////////////////////////////

  protected void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    // launch async commit worker if needed
    launchAsyncCommitWorkerIfNeeded();

    asyncCommitExecutor.submit(new AsyncCommitWorker(messages, callback));
  }

  protected CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages) {
    // launch async commit worker if needed
    launchAsyncCommitWorkerIfNeeded();

    final CompletableFuture<Void> future = new CompletableFuture<>();
    asyncCommitExecutor.submit(
        () -> {
          try {
            ack(messages);
            future.complete(null);
          } catch (final Throwable e) {
            future.completeExceptionally(e);
          }
        });
    return future;
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
    throw new SubscriptionRuntimeCriticalException(errorMessage);
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
    throw new SubscriptionRuntimeCriticalException(errorMessage);
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
    throw new SubscriptionRuntimeCriticalException(errorMessage);
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

    protected String fileSaveDir = ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE;

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

    public Builder fileSaveDir(final String fileSaveDir) {
      this.fileSaveDir = fileSaveDir;
      return this;
    }

    public abstract SubscriptionPullConsumer buildPullConsumer();

    public abstract SubscriptionPushConsumer buildPushConsumer();
  }

  /////////////////////////////// commit async worker ///////////////////////////////

  private void launchAsyncCommitWorkerIfNeeded() {
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
  }

  private void shutdownAsyncCommitWorkerIfNeeded() {
    if (asyncCommitExecutor != null) {
      asyncCommitExecutor.shutdown();
      asyncCommitExecutor = null;
    }
  }

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
        ack(messages);
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

  /////////////////////////////// utility ///////////////////////////////

  protected long generateRandomInitialDelayMs(final long maxMs) {
    return (long) (Math.random() * maxMs);
  }
}
