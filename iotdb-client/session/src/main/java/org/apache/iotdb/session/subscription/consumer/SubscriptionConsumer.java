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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPipeTimeoutException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPollTimeoutException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionTimeoutException;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;
import org.apache.iotdb.session.subscription.util.PollTimer;
import org.apache.iotdb.session.subscription.util.RandomStringGenerator;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URLEncoder;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.config.TopicConstant.MODE_SNAPSHOT_VALUE;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.ERROR;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.FILE_INIT;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.TABLETS;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.TERMINATION;
import static org.apache.iotdb.session.subscription.util.SetPartitioner.partition;

abstract class SubscriptionConsumer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumer.class);

  private static final long SLEEP_MS = 100L;
  private static final long SLEEP_DELTA_MS = 50L;
  private static final long TIMER_DELTA_MS = 250L;

  private final String username;
  private final String password;

  protected String consumerId;
  protected String consumerGroupId;

  private final long heartbeatIntervalMs;
  private final long endpointsSyncIntervalMs;

  private final SubscriptionProviders providers;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);
  // This variable indicates whether the consumer has ever been closed.
  private final AtomicBoolean isReleased = new AtomicBoolean(false);

  private final String fileSaveDir;
  private final boolean fileSaveFsync;
  private final Set<SubscriptionCommitContext> inFlightFilesCommitContextSet = new HashSet<>();

  private final int thriftMaxFrameSize;
  private final int maxPollParallelism;

  @SuppressWarnings("java:S3077")
  protected volatile Map<String, TopicConfig> subscribedTopics = new HashMap<>();

  public boolean allSnapshotTopicMessagesHaveBeenConsumed() {
    return subscribedTopics.values().stream()
        .noneMatch(
            (config) -> config.getAttributesWithSourceMode().containsValue(MODE_SNAPSHOT_VALUE));
  }

  /////////////////////////////// getter ///////////////////////////////

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionConsumer(final Builder builder) {
    final Set<TEndPoint> initialEndpoints = new HashSet<>();
    // From org.apache.iotdb.session.Session.getNodeUrls
    // Priority is given to `host:port` over `nodeUrls`.
    if (Objects.nonNull(builder.host) || Objects.nonNull(builder.port)) {
      if (Objects.isNull(builder.host)) {
        builder.host = SessionConfig.DEFAULT_HOST;
      }
      if (Objects.isNull(builder.port)) {
        builder.port = SessionConfig.DEFAULT_PORT;
      }
      initialEndpoints.add(new TEndPoint(builder.host, builder.port));
    } else if (Objects.isNull(builder.nodeUrls)) {
      builder.host = SessionConfig.DEFAULT_HOST;
      builder.port = SessionConfig.DEFAULT_PORT;
      initialEndpoints.add(new TEndPoint(builder.host, builder.port));
    } else {
      initialEndpoints.addAll(SessionUtils.parseSeedNodeUrls(builder.nodeUrls));
    }
    this.providers = new SubscriptionProviders(initialEndpoints);

    this.username = builder.username;
    this.password = builder.password;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;

    this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
    this.endpointsSyncIntervalMs = builder.endpointsSyncIntervalMs;

    this.fileSaveDir = builder.fileSaveDir;
    this.fileSaveFsync = builder.fileSaveFsync;

    this.thriftMaxFrameSize = builder.thriftMaxFrameSize;
    this.maxPollParallelism = builder.maxPollParallelism;
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
                        ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE))
            .fileSaveFsync(
                (Boolean)
                    properties.getOrDefault(
                        ConsumerConstant.FILE_SAVE_FSYNC_KEY,
                        ConsumerConstant.FILE_SAVE_FSYNC_DEFAULT_VALUE))
            .thriftMaxFrameSize(
                (Integer)
                    properties.getOrDefault(
                        ConsumerConstant.THRIFT_MAX_FRAME_SIZE_KEY,
                        SessionConfig.DEFAULT_MAX_FRAME_SIZE))
            .maxPollParallelism(
                (Integer)
                    properties.getOrDefault(
                        ConsumerConstant.MAX_POLL_PARALLELISM_KEY,
                        ConsumerConstant.MAX_POLL_PARALLELISM_DEFAULT_VALUE)));
  }

  /////////////////////////////// open & close ///////////////////////////////

  private void checkIfHasBeenClosed() throws SubscriptionException {
    if (isReleased.get()) {
      final String errorMessage =
          String.format("%s has ever been closed, unsupported operation after closing.", this);
      LOGGER.error(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
  }

  private void checkIfOpened() throws SubscriptionException {
    if (isClosed.get()) {
      final String errorMessage =
          String.format("%s is not yet open, please open the subscription consumer first.", this);
      LOGGER.error(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
  }

  public synchronized void open() throws SubscriptionException {
    checkIfHasBeenClosed();

    if (!isClosed.get()) {
      return;
    }

    // open subscription providers
    providers.acquireWriteLock();
    try {
      providers.openProviders(this); // throw SubscriptionException
    } finally {
      providers.releaseWriteLock();
    }

    // set isClosed to false before submitting workers
    isClosed.set(false);

    // submit heartbeat worker
    submitHeartbeatWorker();

    // submit endpoints syncer
    submitEndpointsSyncer();
  }

  @Override
  public synchronized void close() {
    if (isClosed.get()) {
      return;
    }

    // close subscription providers
    providers.acquireWriteLock();
    providers.closeProviders();
    providers.releaseWriteLock();

    isClosed.set(true);

    // mark is released to avoid reopening after closing
    isReleased.set(true);
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
    // parse topic names from external source
    subscribe(topicNames, true);
  }

  private void subscribe(Set<String> topicNames, final boolean needParse)
      throws SubscriptionException {
    checkIfOpened();

    if (needParse) {
      topicNames =
          topicNames.stream().map(IdentifierUtils::parseIdentifier).collect(Collectors.toSet());
    }

    providers.acquireReadLock();
    try {
      subscribeWithRedirection(topicNames);
    } finally {
      providers.releaseReadLock();
    }
  }

  public void unsubscribe(final String topicName) throws SubscriptionException {
    unsubscribe(Collections.singleton(topicName));
  }

  public void unsubscribe(final String... topicNames) throws SubscriptionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  public void unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    // parse topic names from external source
    unsubscribe(topicNames, true);
  }

  private void unsubscribe(Set<String> topicNames, final boolean needParse)
      throws SubscriptionException {
    checkIfOpened();

    if (needParse) {
      topicNames =
          topicNames.stream().map(IdentifierUtils::parseIdentifier).collect(Collectors.toSet());
    }

    providers.acquireReadLock();
    try {
      unsubscribeWithRedirection(topicNames);
    } finally {
      providers.releaseReadLock();
    }
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  SubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
      throws SubscriptionException {
    final SubscriptionProvider provider =
        new SubscriptionProvider(
            endPoint,
            this.username,
            this.password,
            this.consumerId,
            this.consumerGroupId,
            this.thriftMaxFrameSize);
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

  /////////////////////////////// file ops ///////////////////////////////

  private Path getFileDir(final String topicName) throws IOException {
    final Path dirPath =
        Paths.get(fileSaveDir).resolve(consumerGroupId).resolve(consumerId).resolve(topicName);
    Files.createDirectories(dirPath);
    return dirPath;
  }

  private Path getFilePath(
      final SubscriptionCommitContext commitContext,
      final String topicName,
      final String fileName,
      final boolean allowFileAlreadyExistsException,
      final boolean allowInvalidPathException)
      throws SubscriptionException {
    try {
      final Path filePath;
      try {
        filePath = getFileDir(topicName).resolve(fileName);
      } catch (final InvalidPathException invalidPathException) {
        if (allowInvalidPathException) {
          return getFilePath(commitContext, URLEncoder.encode(topicName), fileName, true, false);
        }
        throw new SubscriptionRuntimeNonCriticalException(
            invalidPathException.getMessage(), invalidPathException);
      }

      try {
        Files.createFile(filePath);
        return filePath;
      } catch (final FileAlreadyExistsException fileAlreadyExistsException) {
        if (allowFileAlreadyExistsException) {
          if (inFlightFilesCommitContextSet.contains(commitContext)) {
            LOGGER.info(
                "Detect already existed file {} when polling topic {}, resume consumption",
                fileName,
                topicName);
            return filePath;
          }
          final String suffix = RandomStringGenerator.generate(16);
          LOGGER.warn(
              "Detect already existed file {} when polling topic {}, add random suffix {} to filename",
              fileName,
              topicName,
              suffix);
          return getFilePath(commitContext, topicName, fileName + "." + suffix, false, true);
        }
        throw new SubscriptionRuntimeNonCriticalException(
            fileAlreadyExistsException.getMessage(), fileAlreadyExistsException);
      }
    } catch (final IOException e) {
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  /////////////////////////////// poll ///////////////////////////////

  private final Map<
          SubscriptionPollResponseType,
          BiFunction<SubscriptionPollResponse, PollTimer, Optional<SubscriptionMessage>>>
      responseTransformer =
          Collections.unmodifiableMap(
              new HashMap<
                  SubscriptionPollResponseType,
                  BiFunction<
                      SubscriptionPollResponse, PollTimer, Optional<SubscriptionMessage>>>() {
                {
                  put(TABLETS, (resp, timer) -> pollTablets(resp, timer));
                  put(FILE_INIT, (resp, timer) -> pollFile(resp, timer));
                  put(
                      ERROR,
                      (resp, timer) -> {
                        final ErrorPayload payload = (ErrorPayload) resp.getPayload();
                        final String errorMessage = payload.getErrorMessage();
                        if (payload.isCritical()) {
                          throw new SubscriptionRuntimeCriticalException(errorMessage);
                        } else {
                          throw new SubscriptionRuntimeNonCriticalException(errorMessage);
                        }
                      });
                  put(
                      TERMINATION,
                      (resp, timer) -> {
                        final SubscriptionCommitContext commitContext = resp.getCommitContext();
                        final String topicNameToUnsubscribe = commitContext.getTopicName();
                        LOGGER.info(
                            "Termination occurred when SubscriptionConsumer {} polling topics, unsubscribe topic {} automatically",
                            this,
                            topicNameToUnsubscribe);
                        unsubscribe(Collections.singleton(topicNameToUnsubscribe), false);
                        return Optional.empty();
                      });
                }
              });

  protected List<SubscriptionMessage> multiplePoll(
      /* @NotNull */ final Set<String> topicNames, final long timeoutMs) {
    if (topicNames.isEmpty()) {
      return Collections.emptyList();
    }

    // execute single task in current thread
    final int availableCount =
        SubscriptionExecutorServiceManager.getAvailableThreadCountForPollTasks();
    if (availableCount == 0) {
      // non-strict timeout
      return singlePoll(topicNames, timeoutMs);
    }

    // dividing topics
    final List<PollTask> tasks = new ArrayList<>();
    final List<Set<String>> partitionedTopicNames =
        partition(topicNames, Math.min(maxPollParallelism, availableCount));
    for (final Set<String> partition : partitionedTopicNames) {
      tasks.add(new PollTask(partition, timeoutMs));
    }

    // submit multiple tasks to poll messages
    final List<SubscriptionMessage> messages = new ArrayList<>();
    SubscriptionRuntimeCriticalException lastSubscriptionRuntimeCriticalException = null;
    try {
      // strict timeout
      for (final Future<List<SubscriptionMessage>> future :
          SubscriptionExecutorServiceManager.submitMultiplePollTasks(tasks, timeoutMs)) {
        try {
          if (future.isCancelled()) {
            continue;
          }
          messages.addAll(future.get());
        } catch (final CancellationException ignored) {

        } catch (final ExecutionException e) {
          final Throwable cause = e.getCause();
          if (cause instanceof SubscriptionRuntimeCriticalException) {
            final SubscriptionRuntimeCriticalException ex =
                (SubscriptionRuntimeCriticalException) cause;
            LOGGER.warn(
                "SubscriptionRuntimeCriticalException occurred when SubscriptionConsumer {} polling topics {}",
                this,
                topicNames,
                ex);
            lastSubscriptionRuntimeCriticalException = ex;
          } else {
            LOGGER.warn(
                "ExecutionException occurred when SubscriptionConsumer {} polling topics {}",
                this,
                topicNames,
                e);
          }
        }
      }
    } catch (final InterruptedException e) {
      LOGGER.warn(
          "InterruptedException occurred when SubscriptionConsumer {} polling topics {}",
          this,
          topicNames,
          e);
      Thread.currentThread().interrupt(); // restore interrupted state
    }

    // TODO: ignore possible interrupted state?

    // even if a SubscriptionRuntimeCriticalException is encountered, try to deliver the message to
    // the client
    if (messages.isEmpty() && Objects.nonNull(lastSubscriptionRuntimeCriticalException)) {
      throw lastSubscriptionRuntimeCriticalException;
    }

    return messages;
  }

  private class PollTask implements Callable<List<SubscriptionMessage>> {

    private final Set<String> topicNames;
    private final long timeoutMs;

    public PollTask(final Set<String> topicNames, final long timeoutMs) {
      this.topicNames = topicNames;
      this.timeoutMs = timeoutMs;
    }

    @Override
    public List<SubscriptionMessage> call() {
      return singlePoll(topicNames, timeoutMs);
    }
  }

  private List<SubscriptionMessage> singlePoll(
      /* @NotNull */ final Set<String> topicNames, final long timeoutMs)
      throws SubscriptionException {
    if (topicNames.isEmpty()) {
      return Collections.emptyList();
    }

    final List<SubscriptionMessage> messages = new ArrayList<>();
    List<SubscriptionPollResponse> currentResponses = new ArrayList<>();
    final PollTimer timer = new PollTimer(System.currentTimeMillis(), timeoutMs);

    try {
      do {
        final List<SubscriptionMessage> currentMessages = new ArrayList<>();
        try {
          currentResponses.clear();
          currentResponses = pollInternal(topicNames, timer.remainingMs());
          for (final SubscriptionPollResponse response : currentResponses) {
            final short responseType = response.getResponseType();
            if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
              LOGGER.warn("unexpected response type: {}", responseType);
              continue;
            }
            try {
              responseTransformer
                  .getOrDefault(
                      SubscriptionPollResponseType.valueOf(responseType),
                      (resp, ignored) -> {
                        LOGGER.warn("unexpected response type: {}", responseType);
                        return Optional.empty();
                      })
                  .apply(response, timer)
                  .ifPresent(currentMessages::add);
            } catch (final SubscriptionRuntimeNonCriticalException e) {
              LOGGER.warn(
                  "SubscriptionRuntimeNonCriticalException occurred when SubscriptionConsumer {} polling topics {}",
                  this,
                  topicNames,
                  e);
              // assume the corresponding response has been nacked
            }
          }
        } catch (final SubscriptionRuntimeCriticalException e) {
          LOGGER.warn(
              "SubscriptionRuntimeCriticalException occurred when SubscriptionConsumer {} polling topics {}",
              this,
              topicNames,
              e);
          // nack and clear current responses
          try {
            nack(currentResponses);
            currentResponses.clear();
          } catch (final Exception ignored) {
          }
          // nack and clear result messages
          try {
            nack(messages);
            messages.clear();
          } catch (final Exception ignored) {
          }

          // the upper layer perceives ExecutionException
          throw e;
        }

        // add all current messages to result messages
        messages.addAll(currentMessages);

        // TODO: maybe we can poll a few more times
        if (!messages.isEmpty()) {
          break;
        }

        // update timer
        timer.update();

        // TODO: associated with timeoutMs instead of hardcoding
        // random sleep time within the range [SLEEP_DELTA_MS, SLEEP_DELTA_MS + SLEEP_MS)
        Thread.sleep(((long) (Math.random() * SLEEP_MS)) + SLEEP_DELTA_MS);

        // the use of TIMER_DELTA_MS here slightly reduces the timeout to avoid being interrupted as
        // much as possible
      } while (timer.notExpired(TIMER_DELTA_MS));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt(); // restore interrupted state
    }

    if (Thread.currentThread().isInterrupted()) {
      // nack and clear current responses
      try {
        nack(currentResponses);
        currentResponses.clear();
      } catch (final Exception ignored) {
      }
      // nack and clear result messages
      try {
        nack(messages);
        messages.clear();
      } catch (final Exception ignored) {
      }

      // the upper layer perceives CancellationException
      return Collections.emptyList();
    }

    return messages;
  }

  private Optional<SubscriptionMessage> pollFile(
      final SubscriptionPollResponse response, final PollTimer timer) throws SubscriptionException {
    final SubscriptionCommitContext commitContext = response.getCommitContext();
    final String fileName = ((FileInitPayload) response.getPayload()).getFileName();
    final String topicName = commitContext.getTopicName();
    final Path filePath = getFilePath(commitContext, topicName, fileName, true, true);
    final File file = filePath.toFile();
    try (final RandomAccessFile fileWriter = new RandomAccessFile(file, "rw")) {
      return Optional.of(pollFileInternal(commitContext, fileName, file, fileWriter, timer));
    } catch (final Exception e) {
      if (!(e instanceof SubscriptionPollTimeoutException)) {
        inFlightFilesCommitContextSet.remove(commitContext);
      }
      // construct temporary message to nack
      nack(
          Collections.singletonList(
              new SubscriptionMessage(commitContext, file.getAbsolutePath())));
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  private SubscriptionMessage pollFileInternal(
      final SubscriptionCommitContext commitContext,
      final String rawFileName,
      final File file,
      final RandomAccessFile fileWriter,
      final PollTimer timer)
      throws IOException, SubscriptionException {
    long writingOffset = fileWriter.length();

    LOGGER.info(
        "{} start to poll file {} with commit context {} at offset {}",
        this,
        file.getAbsolutePath(),
        commitContext,
        writingOffset);

    fileWriter.seek(writingOffset);
    while (true) {
      timer.update();
      if (timer.isExpired(TIMER_DELTA_MS)) {
        // resume from breakpoint if timeout happened when polling files
        inFlightFilesCommitContextSet.add(commitContext);
        final String message =
            String.format(
                "Timeout occurred when SubscriptionConsumer %s polling file %s with commit context %s, record writing offset %s for subsequent poll",
                this, file.getAbsolutePath(), commitContext, writingOffset);
        LOGGER.info(message);
        throw new SubscriptionRuntimeNonCriticalException(message);
      }

      final List<SubscriptionPollResponse> responses =
          pollFileInternal(commitContext, writingOffset, timer.remainingMs());

      // It's agreed that the server will always return at least one response, even in case of
      // failure.
      if (responses.isEmpty()) {
        final String errorMessage =
            String.format("SubscriptionConsumer %s poll empty response", this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      // only one SubscriptionEvent polled currently
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
            if (!Objects.equals(rawFileName, ((FilePiecePayload) payload).getFileName())) {
              final String errorMessage =
                  String.format(
                      "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                      rawFileName, ((FilePiecePayload) payload).getFileName(), this);
              LOGGER.warn(errorMessage);
              throw new SubscriptionRuntimeNonCriticalException(errorMessage);
            }

            // write file piece
            fileWriter.write(((FilePiecePayload) payload).getFilePiece());
            if (fileSaveFsync) {
              fileWriter.getFD().sync();
            }

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
            if (!Objects.equals(rawFileName, ((FileSealPayload) payload).getFileName())) {
              final String errorMessage =
                  String.format(
                      "inconsistent file name, current is %s, incoming is %s, consumer: %s",
                      rawFileName, ((FileSealPayload) payload).getFileName(), this);
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

            // optional sync and close
            if (fileSaveFsync) {
              fileWriter.getFD().sync();
            }
            fileWriter.close();

            LOGGER.info(
                "SubscriptionConsumer {} successfully poll file {} with commit context {}",
                this,
                file.getAbsolutePath(),
                commitContext);

            // generate subscription message
            inFlightFilesCommitContextSet.remove(commitContext);
            return new SubscriptionMessage(commitContext, file.getAbsolutePath());
          }
        case ERROR:
          {
            // no need to check commit context

            final String errorMessage = ((ErrorPayload) payload).getErrorMessage();
            final boolean critical = ((ErrorPayload) payload).isCritical();
            if (!critical
                && Objects.nonNull(errorMessage)
                && errorMessage.contains(SubscriptionTimeoutException.KEYWORD)) {
              // resume from breakpoint if timeout happened when polling files
              inFlightFilesCommitContextSet.add(commitContext);
              final String message =
                  String.format(
                      "Timeout occurred when SubscriptionConsumer %s polling file %s with commit context %s, record writing offset %s for subsequent poll",
                      this, file.getAbsolutePath(), commitContext, writingOffset);
              LOGGER.info(message);
              throw new SubscriptionPollTimeoutException(message);
            } else {
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
          }
        default:
          final String errorMessage = String.format("unexpected response type: %s", responseType);
          LOGGER.warn(errorMessage);
          throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }
    }
  }

  private Optional<SubscriptionMessage> pollTablets(
      final SubscriptionPollResponse response, final PollTimer timer) throws SubscriptionException {
    try {
      return pollTabletsInternal(response, timer);
    } catch (final Exception e) {
      // construct temporary message to nack
      nack(
          Collections.singletonList(
              new SubscriptionMessage(response.getCommitContext(), Collections.emptyList())));
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  private Optional<SubscriptionMessage> pollTabletsInternal(
      final SubscriptionPollResponse initialResponse, final PollTimer timer) {
    final List<Tablet> tablets = ((TabletsPayload) initialResponse.getPayload()).getTablets();
    final SubscriptionCommitContext commitContext = initialResponse.getCommitContext();

    int nextOffset = ((TabletsPayload) initialResponse.getPayload()).getNextOffset();
    while (true) {
      if (nextOffset < 0) {
        if (!Objects.equals(tablets.size(), -nextOffset)) {
          final String errorMessage =
              String.format(
                  "inconsistent tablet size, current is %s, incoming is %s, consumer: %s",
                  tablets.size(), -nextOffset, this);
          LOGGER.warn(errorMessage);
          throw new SubscriptionRuntimeNonCriticalException(errorMessage);
        }
        return Optional.of(new SubscriptionMessage(commitContext, tablets));
      }

      timer.update();
      if (timer.isExpired(TIMER_DELTA_MS)) {
        final String errorMessage =
            String.format(
                "timeout while poll tablets with commit context: %s, consumer: %s",
                commitContext, this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      final List<SubscriptionPollResponse> responses =
          pollTabletsInternal(commitContext, nextOffset, timer.remainingMs());

      // It's agreed that the server will always return at least one response, even in case of
      // failure.
      if (responses.isEmpty()) {
        final String errorMessage =
            String.format("SubscriptionConsumer %s poll empty response", this);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      // only one SubscriptionEvent polled currently
      final SubscriptionPollResponse response = responses.get(0);
      final SubscriptionPollPayload payload = response.getPayload();
      final short responseType = response.getResponseType();
      if (!SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
        final String errorMessage = String.format("unexpected response type: %s", responseType);
        LOGGER.warn(errorMessage);
        throw new SubscriptionRuntimeNonCriticalException(errorMessage);
      }

      switch (SubscriptionPollResponseType.valueOf(responseType)) {
        case TABLETS:
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

            // update tablets
            tablets.addAll(((TabletsPayload) response.getPayload()).getTablets());

            // update offset
            nextOffset = ((TabletsPayload) payload).getNextOffset();
            break;
          }
        case ERROR:
          {
            // no need to check commit context

            final String errorMessage = ((ErrorPayload) payload).getErrorMessage();
            final boolean critical = ((ErrorPayload) payload).isCritical();
            LOGGER.warn(
                "Error occurred when SubscriptionConsumer {} polling tablets with commit context {}: {}, critical: {}",
                this,
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

  private List<SubscriptionPollResponse> pollInternal(
      final Set<String> topicNames, final long timeoutMs) throws SubscriptionException {
    providers.acquireReadLock();
    try {
      final SubscriptionProvider provider = providers.getNextAvailableProvider();
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                "Cluster has no available subscription providers when %s poll topic %s",
                this, topicNames));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.poll(topicNames, timeoutMs);
      } catch (final SubscriptionConnectionException ignored) {
        return Collections.emptyList();
      }
    } finally {
      providers.releaseReadLock();
    }
  }

  private List<SubscriptionPollResponse> pollFileInternal(
      final SubscriptionCommitContext commitContext, final long writingOffset, final long timeoutMs)
      throws SubscriptionException {
    final int dataNodeId = commitContext.getDataNodeId();
    providers.acquireReadLock();
    try {
      final SubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when %s poll file from subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                this, dataNodeId));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.pollFile(commitContext, writingOffset, timeoutMs);
      } catch (final SubscriptionConnectionException ignored) {
        return Collections.emptyList();
      }
    } finally {
      providers.releaseReadLock();
    }
  }

  private List<SubscriptionPollResponse> pollTabletsInternal(
      final SubscriptionCommitContext commitContext, final int offset, final long timeoutMs)
      throws SubscriptionException {
    final int dataNodeId = commitContext.getDataNodeId();
    providers.acquireReadLock();
    try {
      final SubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when %s poll tablets from subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                this, dataNodeId));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.pollTablets(commitContext, offset, timeoutMs);
      } catch (final SubscriptionConnectionException ignored) {
        return Collections.emptyList();
      }
    } finally {
      providers.releaseReadLock();
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
    for (final Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitInternal(entry.getKey(), entry.getValue(), false);
    }
  }

  protected void nack(final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionMessage message : messages) {
      // make every effort to delete stale intermediate file
      if (Objects.equals(
              SubscriptionMessageType.TS_FILE_HANDLER.getType(), message.getMessageType())
          &&
          // do not delete file that can resume from breakpoint
          !inFlightFilesCommitContextSet.contains(message.getCommitContext())) {
        try {
          message.getTsFileHandler().deleteFile();
        } catch (final Exception ignored) {
        }
      }
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(message.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(message.getCommitContext());
    }
    for (final Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitInternal(entry.getKey(), entry.getValue(), true);
    }
  }

  private void nack(final List<SubscriptionPollResponse> responses) throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionPollResponse response : responses) {
      // there is no stale intermediate file here
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(response.getCommitContext().getDataNodeId(), (id) -> new ArrayList<>())
          .add(response.getCommitContext());
    }
    for (final Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      commitInternal(entry.getKey(), entry.getValue(), true);
    }
  }

  private void commitInternal(
      final int dataNodeId,
      final List<SubscriptionCommitContext> subscriptionCommitContexts,
      final boolean nack)
      throws SubscriptionException {
    providers.acquireReadLock();
    try {
      final SubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return;
        }
        throw new SubscriptionConnectionException(
            String.format(
                "something unexpected happened when %s commit (nack: %s) messages to subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                this, nack, dataNodeId));
      }
      provider.commit(subscriptionCommitContexts, nack);
    } finally {
      providers.releaseReadLock();
    }
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  private void submitHeartbeatWorker() {
    final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
    future[0] =
        SubscriptionExecutorServiceManager.submitHeartbeatWorker(
            () -> {
              if (isClosed()) {
                if (Objects.nonNull(future[0])) {
                  future[0].cancel(false);
                  LOGGER.info("SubscriptionConsumer {} cancel heartbeat worker", this);
                }
                return;
              }
              providers.heartbeat(this);
            },
            heartbeatIntervalMs);
    LOGGER.info("SubscriptionConsumer {} submit heartbeat worker", this);
  }

  /////////////////////////////// sync endpoints ///////////////////////////////

  private void submitEndpointsSyncer() {
    final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
    future[0] =
        SubscriptionExecutorServiceManager.submitEndpointsSyncer(
            () -> {
              if (isClosed()) {
                if (Objects.nonNull(future[0])) {
                  future[0].cancel(false);
                  LOGGER.info("SubscriptionConsumer {} cancel endpoints syncer", this);
                }
                return;
              }
              providers.sync(this);
            },
            endpointsSyncIntervalMs);
    LOGGER.info("SubscriptionConsumer {} submit endpoints syncer", this);
  }

  /////////////////////////////// commit async ///////////////////////////////

  protected void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    SubscriptionExecutorServiceManager.submitAsyncCommitWorker(
        new AsyncCommitWorker(messages, callback));
  }

  private class AsyncCommitWorker implements Runnable {

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

  protected CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages) {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    SubscriptionExecutorServiceManager.submitAsyncCommitWorker(
        () -> {
          if (isClosed()) {
            return;
          }

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
    final List<SubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers when %s subscribe topic %s",
              this, topicNames));
    }
    for (final SubscriptionProvider provider : providers) {
      try {
        subscribedTopics = provider.subscribe(topicNames);
        return;
      } catch (final Exception e) {
        if (e instanceof SubscriptionPipeTimeoutException) {
          // degrade exception to log for pipe timeout
          LOGGER.warn(e.getMessage());
          return;
        }
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
    final List<SubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              "Cluster has no available subscription providers when %s unsubscribe topic %s",
              this, topicNames));
    }
    for (final SubscriptionProvider provider : providers) {
      try {
        subscribedTopics = provider.unsubscribe(topicNames);
        return;
      } catch (final Exception e) {
        if (e instanceof SubscriptionPipeTimeoutException) {
          // degrade exception to log for pipe timeout
          LOGGER.warn(e.getMessage());
          return;
        }
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
    final List<SubscriptionProvider> providers = this.providers.getAllAvailableProviders();
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

    protected String host;
    protected Integer port;
    protected List<String> nodeUrls;

    protected String username = SessionConfig.DEFAULT_USER;
    protected String password = SessionConfig.DEFAULT_PASSWORD;

    protected String consumerId;
    protected String consumerGroupId;

    protected long heartbeatIntervalMs = ConsumerConstant.HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE;
    protected long endpointsSyncIntervalMs =
        ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE;

    protected String fileSaveDir = ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE;
    protected boolean fileSaveFsync = ConsumerConstant.FILE_SAVE_FSYNC_DEFAULT_VALUE;

    protected int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    protected int maxPollParallelism = ConsumerConstant.MAX_POLL_PARALLELISM_DEFAULT_VALUE;

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
      this.consumerId = IdentifierUtils.parseIdentifier(consumerId);
      return this;
    }

    public Builder consumerGroupId(final String consumerGroupId) {
      this.consumerGroupId = IdentifierUtils.parseIdentifier(consumerGroupId);
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

    public Builder fileSaveFsync(final boolean fileSaveFsync) {
      this.fileSaveFsync = fileSaveFsync;
      return this;
    }

    public Builder thriftMaxFrameSize(final int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Builder maxPollParallelism(final int maxPollParallelism) {
      // Here the minimum value of max poll parallelism is set to 1 instead of 0, in order to use a
      // single thread to execute poll whenever there are idle resources available, thereby
      // achieving strict timeout.
      this.maxPollParallelism = Math.max(maxPollParallelism, 1);
      return this;
    }

    public abstract SubscriptionPullConsumer buildPullConsumer();

    public abstract SubscriptionPushConsumer buildPushConsumer();
  }

  /////////////////////////////// stringify ///////////////////////////////

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("consumerId", consumerId);
    result.put("consumerGroupId", consumerGroupId);
    result.put("isClosed", isClosed.toString());
    result.put("fileSaveDir", fileSaveDir);
    result.put("inFlightFilesCommitContextSet", inFlightFilesCommitContextSet.toString());
    result.put("subscribedTopicNames", subscribedTopics.keySet().toString());
    return result;
  }

  protected Map<String, String> allReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("consumerId", consumerId);
    result.put("consumerGroupId", consumerGroupId);
    result.put("heartbeatIntervalMs", String.valueOf(heartbeatIntervalMs));
    result.put("endpointsSyncIntervalMs", String.valueOf(endpointsSyncIntervalMs));
    result.put("providers", providers.toString());
    result.put("isClosed", isClosed.toString());
    result.put("isReleased", isReleased.toString());
    result.put("fileSaveDir", fileSaveDir);
    result.put("fileSaveFsync", String.valueOf(fileSaveFsync));
    result.put("inFlightFilesCommitContextSet", inFlightFilesCommitContextSet.toString());
    result.put("thriftMaxFrameSize", String.valueOf(thriftMaxFrameSize));
    result.put("maxPollParallelism", String.valueOf(maxPollParallelism));
    result.put("subscribedTopics", subscribedTopics.toString());
    return result;
  }
}
