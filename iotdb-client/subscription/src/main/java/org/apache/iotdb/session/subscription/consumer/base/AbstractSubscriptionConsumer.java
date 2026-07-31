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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionOwnerFencedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPipeTimeoutException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionPollTimeoutException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionTimeoutException;
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.FileSealPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WatermarkPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionSeekReq;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.util.CollectionUtils;
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
import java.util.Collection;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.ERROR;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.FILE_INIT;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.TABLETS;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.TERMINATION;
import static org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType.WATERMARK;
import static org.apache.iotdb.session.subscription.util.SetPartitioner.partition;

abstract class AbstractSubscriptionConsumer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSubscriptionConsumer.class);

  private static final long SLEEP_MS = 100L;
  private static final long SLEEP_DELTA_MS = 50L;
  private static final long TIMER_DELTA_MS = 250L;

  private final String username;
  private final String password;
  private final String encryptedPassword;

  protected String consumerId;
  protected String consumerGroupId;
  protected String ownerId;
  protected Long ownerEpoch;

  private final long heartbeatIntervalMs;
  private final long endpointsSyncIntervalMs;

  private final AbstractSubscriptionProviders providers;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);
  // This variable indicates whether the consumer has ever been closed.
  private final AtomicBoolean isReleased = new AtomicBoolean(false);

  private final String fileSaveDir;
  private final boolean fileSaveFsync;
  private final Set<SubscriptionCommitContext> inFlightFilesCommitContextSet = new HashSet<>();

  private final int thriftMaxFrameSize;
  private final int connectionTimeoutInMs;
  private final int maxPollParallelism;

  /**
   * The latest watermark timestamp received from the server. Updated when WATERMARK events are
   * processed and stripped. Consumer users can query this to check timestamp progress.
   */
  protected volatile long latestWatermarkTimestamp = Long.MIN_VALUE;

  /** Per-topic current positions used as the consumer-guided positioning hint in poll requests. */
  private final Map<String, TopicProgress> currentPositionsByTopic = new ConcurrentHashMap<>();

  /** Per-topic committed positions used as durable recovery points for explicit seek/checkpoint. */
  private final Map<String, TopicProgress> committedPositionsByTopic = new ConcurrentHashMap<>();

  /**
   * Ack contexts for consensus messages that were already processed locally but could not be
   * committed because the original provider became unavailable. They are flushed after the same
   * topic+region is observed again from a live provider.
   */
  private final Map<String, Set<SubscriptionCommitContext>> pendingRedirectAcksByTopicRegion =
      new ConcurrentHashMap<>();

  @SuppressWarnings("java:S3077")
  protected volatile Map<String, TopicConfig> subscribedTopics = new HashMap<>();

  @Deprecated
  public boolean allSnapshotTopicMessagesHaveBeenConsumed() {
    return allTopicMessagesHaveBeenConsumed(subscribedTopics.keySet());
  }

  public boolean allTopicMessagesHaveBeenConsumed() {
    return allTopicMessagesHaveBeenConsumed(subscribedTopics.keySet());
  }

  private boolean allTopicMessagesHaveBeenConsumed(final Collection<String> topicNames) {
    // For the topic that needs to be detected, there are two scenarios to consider:
    //   1. If configs as live, it cannot be determined whether the topic has been fully consumed.
    //   2. If configs as snapshot, it means the topic has not been automatically unsubscribed.
    // Therefore, the logic can be summarized as follows: if there is a matching topic in subscribed
    // topics, then it has not been fully consumed.
    return topicNames.stream().map(subscribedTopics::get).noneMatch(Objects::nonNull);
  }

  /////////////////////////////// getter ///////////////////////////////

  public String getConsumerId() {
    return consumerId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public Long getOwnerEpoch() {
    return ownerEpoch;
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected AbstractSubscriptionConsumer(final AbstractSubscriptionConsumerBuilder builder) {
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
    this.providers = new AbstractSubscriptionProviders(initialEndpoints);

    this.username = builder.username;
    this.password = builder.password;
    this.encryptedPassword = builder.encryptedPassword;

    this.consumerId = builder.consumerId;
    this.consumerGroupId = builder.consumerGroupId;
    this.ownerId = builder.ownerId;
    this.ownerEpoch = builder.ownerEpoch;

    this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
    this.endpointsSyncIntervalMs = builder.endpointsSyncIntervalMs;

    this.fileSaveDir = builder.fileSaveDir;
    this.fileSaveFsync = builder.fileSaveFsync;

    this.thriftMaxFrameSize = builder.thriftMaxFrameSize;
    this.connectionTimeoutInMs = builder.connectionTimeoutInMs;
    this.maxPollParallelism = builder.maxPollParallelism;
  }

  protected AbstractSubscriptionConsumer(
      final AbstractSubscriptionConsumerBuilder builder, final Properties properties) {
    this(
        builder
            .host((String) properties.get(ConsumerConstant.HOST_KEY))
            .port((Integer) properties.get(ConsumerConstant.PORT_KEY))
            .nodeUrls((List<String>) properties.get(ConsumerConstant.NODE_URLS_KEY))
            .username(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.USERNAME_KEY, SessionConfig.DEFAULT_USER))
            .password(
                (String)
                    properties.getOrDefault(
                        ConsumerConstant.PASSWORD_KEY, SessionConfig.DEFAULT_PASSWORD))
            .encryptedPassword((String) properties.get(ConsumerConstant.ENCRYPTED_PASSWORD_KEY))
            .consumerId((String) properties.get(ConsumerConstant.CONSUMER_ID_KEY))
            .consumerGroupId((String) properties.get(ConsumerConstant.CONSUMER_GROUP_ID_KEY))
            .ownerId((String) properties.get(ConsumerConstant.OWNER_ID_KEY))
            .ownerEpoch((Long) properties.get(ConsumerConstant.OWNER_EPOCH_KEY))
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
            .connectionTimeoutInMs(
                (Integer)
                    properties.getOrDefault(
                        ConsumerConstant.CONNECTION_TIMEOUT_MS_KEY,
                        SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS))
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

  protected synchronized void open() throws SubscriptionException {
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
    try {
      providers.closeProviders();
    } finally {
      providers.releaseWriteLock();
    }

    isClosed.set(true);

    // mark is released to avoid reopening after closing
    isReleased.set(true);
  }

  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// subscribe & unsubscribe ///////////////////////////////

  protected void subscribe(final String topicName) throws SubscriptionException {
    subscribe(Collections.singleton(topicName));
  }

  protected void subscribe(final String... topicNames) throws SubscriptionException {
    subscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  protected void subscribe(final Set<String> topicNames) throws SubscriptionException {
    // parse topic names from external source
    subscribe(topicNames, true);
  }

  private void subscribe(Set<String> topicNames, final boolean needParse)
      throws SubscriptionException {
    checkIfOpened();

    if (needParse) {
      topicNames =
          topicNames.stream()
              .map(IdentifierUtils::checkAndParseIdentifier)
              .collect(Collectors.toSet());
    }

    providers.acquireReadLock();
    try {
      subscribeWithRedirection(topicNames);
    } finally {
      providers.releaseReadLock();
    }
  }

  protected void unsubscribe(final String topicName) throws SubscriptionException {
    unsubscribe(Collections.singleton(topicName));
  }

  protected void unsubscribe(final String... topicNames) throws SubscriptionException {
    unsubscribe(new HashSet<>(Arrays.asList(topicNames)));
  }

  protected void unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    // parse topic names from external source
    unsubscribe(topicNames, true);
  }

  private void unsubscribe(Set<String> topicNames, final boolean needParse)
      throws SubscriptionException {
    checkIfOpened();

    if (needParse) {
      topicNames =
          topicNames.stream()
              .map(IdentifierUtils::checkAndParseIdentifier)
              .collect(Collectors.toSet());
    }

    providers.acquireReadLock();
    try {
      unsubscribeWithRedirection(topicNames);
      topicNames.forEach(this::clearPendingRedirectAcks);
    } finally {
      providers.releaseReadLock();
    }
  }

  /////////////////////////////// seek ///////////////////////////////

  /**
   * Seeks to the earliest available WAL position. Actual position depends on WAL retention — old
   * segments may have been reclaimed.
   */
  public void seekToBeginning(final String topicName) throws SubscriptionException {
    checkIfOpened();
    seekInternal(topicName, SubscriptionSeekReq.SEEK_TO_BEGINNING, 0);
    clearCurrentPositions(topicName);
    clearCommittedPositions(topicName);
    clearPendingRedirectAcks(topicName);
  }

  /** Seeks to the current WAL tail. Only newly written data will be consumed after this. */
  public void seekToEnd(final String topicName) throws SubscriptionException {
    checkIfOpened();
    seekInternal(topicName, SubscriptionSeekReq.SEEK_TO_END, 0);
    clearCurrentPositions(topicName);
    clearCommittedPositions(topicName);
    clearPendingRedirectAcks(topicName);
  }

  /**
   * Returns the latest observed per-region positions for the given topic. This is the consumer's
   * current fetch position hint and is sent back to the server on subsequent poll requests.
   */
  public TopicProgress positions(final String topicName) throws SubscriptionException {
    checkIfOpened();
    final TopicProgress progress = currentPositionsByTopic.get(topicName);
    return Objects.nonNull(progress)
        ? new TopicProgress(progress.getRegionProgress())
        : new TopicProgress(Collections.emptyMap());
  }

  /**
   * Returns the latest committed per-region positions for the given topic. This is the recoverable
   * checkpoint position that should be persisted by callers.
   */
  public TopicProgress committedPositions(final String topicName) throws SubscriptionException {
    checkIfOpened();
    final TopicProgress progress = committedPositionsByTopic.get(topicName);
    return Objects.nonNull(progress)
        ? new TopicProgress(progress.getRegionProgress())
        : new TopicProgress(Collections.emptyMap());
  }

  public void seek(final String topicName, final TopicProgress topicProgress)
      throws SubscriptionException {
    checkIfOpened();
    final TopicProgress safeProgress =
        Objects.nonNull(topicProgress) ? topicProgress : new TopicProgress(Collections.emptyMap());
    seekInternalTopicProgress(topicName, safeProgress);
    overlayCurrentPositions(topicName, safeProgress);
    overlayCommittedPositions(topicName, safeProgress);
    clearPendingRedirectAcks(topicName);
  }

  public void seekAfter(final String topicName, final TopicProgress topicProgress)
      throws SubscriptionException {
    checkIfOpened();
    final TopicProgress safeProgress =
        Objects.nonNull(topicProgress) ? topicProgress : new TopicProgress(Collections.emptyMap());
    seekAfterInternalTopicProgress(topicName, safeProgress);
    overlayCurrentPositions(topicName, safeProgress);
    overlayCommittedPositions(topicName, safeProgress);
    clearPendingRedirectAcks(topicName);
  }

  private void seekInternal(final String topicName, final short seekType, final long timestamp)
      throws SubscriptionException {
    providers.acquireReadLock();
    try {
      seekOnAllProviders(topicName, seekType, timestamp);
    } finally {
      providers.releaseReadLock();
    }
  }

  private void seekInternalTopicProgress(final String topicName, final TopicProgress topicProgress)
      throws SubscriptionException {
    providers.acquireReadLock();
    try {
      seekToTopicProgressOnAllProviders(topicName, topicProgress);
    } finally {
      providers.releaseReadLock();
    }
  }

  private void seekAfterInternalTopicProgress(
      final String topicName, final TopicProgress topicProgress) throws SubscriptionException {
    providers.acquireReadLock();
    try {
      seekAfterTopicProgressOnAllProviders(topicName, topicProgress);
    } finally {
      providers.releaseReadLock();
    }
  }

  /////////////////////////////// subscription provider ///////////////////////////////

  protected abstract AbstractSubscriptionProvider constructSubscriptionProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String encryptedPassword,
      final String consumerId,
      final String consumerGroupId,
      final String ownerId,
      final Long ownerEpoch,
      final int thriftMaxFrameSize,
      final long heartbeatIntervalMs,
      final int connectionTimeoutInMs);

  AbstractSubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
      throws SubscriptionException {
    final AbstractSubscriptionProvider provider =
        constructSubscriptionProvider(
            endPoint,
            this.username,
            this.password,
            this.encryptedPassword,
            this.consumerId,
            this.consumerGroupId,
            this.ownerId,
            this.ownerEpoch,
            this.thriftMaxFrameSize,
            this.heartbeatIntervalMs,
            this.connectionTimeoutInMs);
    try {
      provider.handshake();
    } catch (final Exception e) {
      try {
        provider.close();
      } catch (final Exception ignored) {
      }
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_FAILED_HANDSHAKE_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_251C2E2A,
              provider,
              e),
          e);
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
                SubscriptionMessages
                    .LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_RESUME_CONSUMPTION_AD735649,
                fileName,
                topicName);
            return filePath;
          }
          final String suffix = RandomStringGenerator.generate(16);
          LOGGER.warn(
              SubscriptionMessages
                  .LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_ADD_RANDOM_64BBDEEB,
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
                            SubscriptionMessages
                                .LOG_TERMINATION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_UNSUBSCRIBE_TOPIC_ARG_AUTOMATICALLY_E9B695FA,
                            coreReportMessage(),
                            topicNameToUnsubscribe);
                        unsubscribe(Collections.singleton(topicNameToUnsubscribe), false);
                        return Optional.empty();
                      });
                  put(
                      WATERMARK,
                      (resp, timer) -> {
                        final SubscriptionCommitContext commitContext = resp.getCommitContext();
                        final WatermarkPayload payload = (WatermarkPayload) resp.getPayload();
                        return Optional.of(
                            new SubscriptionMessage(
                                commitContext, payload.getWatermarkTimestamp()));
                      });
                }
              });

  /**
   * Returns the set of DataNode IDs for providers that are currently available. Used by subclasses
   * to detect unavailable DataNodes and notify the progress ordering processor.
   */
  protected Set<Integer> getAvailableDataNodeIds() {
    providers.acquireReadLock();
    try {
      final Set<Integer> ids = new HashSet<>();
      for (final AbstractSubscriptionProvider provider : providers.getAllAvailableProviders()) {
        ids.add(provider.getDataNodeId());
      }
      return ids;
    } finally {
      providers.releaseReadLock();
    }
  }

  /**
   * Returns the latest watermark timestamp received from the server. This tracks the maximum data
   * timestamp observed across all polled regions. Returns {@code Long.MIN_VALUE} if no watermark
   * has been received yet.
   */
  public long getLatestWatermarkTimestamp() {
    return latestWatermarkTimestamp;
  }

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
                SubscriptionMessages
                    .LOG_SUBSCRIPTIONRUNTIMECRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_C96324AD,
                this,
                topicNames,
                ex);
            lastSubscriptionRuntimeCriticalException = ex;
          } else {
            LOGGER.warn(
                SubscriptionMessages
                    .LOG_EXECUTIONEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_40F5E1CC,
                this,
                topicNames,
                e);
          }
        }
      }
    } catch (final InterruptedException e) {
      LOGGER.warn(
          SubscriptionMessages
              .LOG_INTERRUPTEDEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_9B556CD5,
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
              LOGGER.warn(SubscriptionMessages.UNEXPECTED_RESPONSE_TYPE_WARN, responseType);
              continue;
            }
            try {
              responseTransformer
                  .getOrDefault(
                      SubscriptionPollResponseType.valueOf(responseType),
                      (resp, ignored) -> {
                        LOGGER.warn(
                            SubscriptionMessages.UNEXPECTED_RESPONSE_TYPE_WARN, responseType);
                        return Optional.empty();
                      })
                  // TODO: reuse previous timer?
                  .apply(response, new PollTimer(System.currentTimeMillis(), timeoutMs))
                  .ifPresent(currentMessages::add);
            } catch (final SubscriptionRuntimeNonCriticalException e) {
              LOGGER.warn(
                  SubscriptionMessages
                      .LOG_SUBSCRIPTIONRUNTIMENONCRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_61838153,
                  this,
                  topicNames,
                  e);
              // assume the corresponding response has been nacked
            }
          }
        } catch (final SubscriptionRuntimeCriticalException e) {
          LOGGER.warn(
              SubscriptionMessages
                  .LOG_SUBSCRIPTIONRUNTIMECRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_C96324AD,
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
        advanceCurrentPositions(currentMessages);
        flushPendingRedirectAcks(currentMessages);

        // TODO: maybe we can poll a few more times
        if (!messages.isEmpty()) {
          break;
        }

        // check if all topic messages have been consumed
        if (allTopicMessagesHaveBeenConsumed(topicNames)) {
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
      return pollFileInternal(commitContext, fileName, file, fileWriter, timer);
    } catch (final Exception e) {
      if (!(e instanceof SubscriptionPollTimeoutException)) {
        inFlightFilesCommitContextSet.remove(commitContext);
      }
      // construct temporary message to nack
      nack(
          Collections.singletonList(
              new SubscriptionMessage(commitContext, file.getAbsolutePath(), null)));
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  private Optional<SubscriptionMessage> pollFileInternal(
      final SubscriptionCommitContext commitContext,
      final String rawFileName,
      final File file,
      final RandomAccessFile fileWriter,
      final PollTimer timer)
      throws IOException, SubscriptionException {
    long writingOffset = fileWriter.length();

    LOGGER.info(
        SubscriptionMessages.LOG_ARG_START_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_AT_OFFSET_0F677E12,
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

      // If responses is empty, it means that some outdated subscription events may be being polled,
      // so just return.
      if (responses.isEmpty()) {
        return Optional.empty();
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
                SubscriptionMessages
                    .LOG_SUBSCRIPTIONCONSUMER_ARG_SUCCESSFULLY_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_A23E0FDB,
                this,
                file.getAbsolutePath(),
                commitContext);

            // generate subscription message
            inFlightFilesCommitContextSet.remove(commitContext);
            return Optional.of(
                new SubscriptionMessage(
                    commitContext,
                    file.getAbsolutePath(),
                    ((FileSealPayload) payload).getDatabaseName(),
                    response.isTimeSelected()));
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
                  SubscriptionMessages
                      .LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_FILE_ARG_COMMIT_CONTEXT_ARG_03619D67,
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
              new SubscriptionMessage(response.getCommitContext(), Collections.emptyMap())));
      throw new SubscriptionRuntimeNonCriticalException(e.getMessage(), e);
    }
  }

  private Optional<SubscriptionMessage> pollTabletsInternal(
      final SubscriptionPollResponse initialResponse, final PollTimer timer) {
    final Map<String, List<Tablet>> tablets =
        ((TabletsPayload) initialResponse.getPayload()).getTabletsWithDBInfo();
    final SubscriptionCommitContext commitContext = initialResponse.getCommitContext();
    boolean timeSelected = initialResponse.isTimeSelected();
    final Map<String, Map<String, Boolean>> timeSelectedByTable = new HashMap<>();
    mergeTimeSelectedByTable(timeSelectedByTable, initialResponse.getTimeSelectedByTable());

    int nextOffset = ((TabletsPayload) initialResponse.getPayload()).getNextOffset();
    while (true) {
      if (nextOffset <= 0) {
        final int tabletsSize = tablets.values().stream().mapToInt(List::size).sum();
        if (!Objects.equals(tabletsSize, -nextOffset)) {
          final String errorMessage =
              String.format(
                  "inconsistent tablet size, current is %s, incoming is %s, consumer: %s",
                  tabletsSize, -nextOffset, this);
          LOGGER.warn(errorMessage);
          throw new SubscriptionRuntimeNonCriticalException(errorMessage);
        }
        return Optional.of(
            new SubscriptionMessage(commitContext, tablets, timeSelected, timeSelectedByTable));
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

      // If responses is empty, it means that some outdated subscription events may be being polled,
      // so just return.
      if (responses.isEmpty()) {
        return Optional.empty();
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
            for (Map.Entry<String, List<Tablet>> entry :
                ((TabletsPayload) response.getPayload()).getTabletsWithDBInfo().entrySet()) {
              tablets
                  .computeIfAbsent(entry.getKey(), databaseName -> new ArrayList<>())
                  .addAll(entry.getValue());
            }

            // update offset
            timeSelected = timeSelected && response.isTimeSelected();
            mergeTimeSelectedByTable(timeSelectedByTable, response.getTimeSelectedByTable());
            nextOffset = ((TabletsPayload) payload).getNextOffset();
            break;
          }
        case ERROR:
          {
            // no need to check commit context

            final String errorMessage = ((ErrorPayload) payload).getErrorMessage();
            final boolean critical = ((ErrorPayload) payload).isCritical();
            if (Objects.equals(payload, ErrorPayload.OUTDATED_ERROR_PAYLOAD)) {
              // suppress warn log when poll outdated subscription event
              return Optional.empty();
            }
            LOGGER.warn(
                SubscriptionMessages
                    .LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TABLETS_COMMIT_CONTEXT_ARG_ARG_FF40B4E1,
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

  private static void mergeTimeSelectedByTable(
      final Map<String, Map<String, Boolean>> target,
      final Map<String, Map<String, Boolean>> source) {
    if (Objects.isNull(source) || source.isEmpty()) {
      return;
    }
    source.forEach(
        (databaseName, tableMap) -> {
          if (Objects.isNull(tableMap) || tableMap.isEmpty()) {
            return;
          }
          target.computeIfAbsent(databaseName, ignored -> new HashMap<>()).putAll(tableMap);
        });
  }

  private List<SubscriptionPollResponse> pollInternal(
      final Set<String> topicNames, final long timeoutMs) throws SubscriptionException {
    providers.acquireReadLock();
    try {
      final AbstractSubscriptionProvider provider = providers.getNextAvailableProvider();
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                SubscriptionMessages
                    .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_POLL_TOPIC_ARG_FABF7A4E,
                this,
                topicNames));
      }
      // ignore SubscriptionConnectionException to improve poll auto retry
      try {
        return provider.poll(topicNames, timeoutMs, buildCurrentProgressByTopic(topicNames));
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
      final AbstractSubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                SubscriptionMessages
                    .EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_FILE_SUBSCRIPTION_PROVIDER_DATA_NODE_07426483,
                this,
                dataNodeId));
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
      final AbstractSubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return Collections.emptyList();
        }
        throw new SubscriptionConnectionException(
            String.format(
                SubscriptionMessages
                    .EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_TABLETS_SUBSCRIPTION_PROVIDER_DATA_NODE_8D80E611,
                this,
                dataNodeId));
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
    ackCommitContexts(extractCommitContexts(messages));
  }

  protected void ackCommitContexts(final Iterable<SubscriptionCommitContext> commitContexts)
      throws SubscriptionException {
    commit(commitContexts, false);
  }

  private Iterable<SubscriptionCommitContext> extractCommitContexts(
      final Iterable<SubscriptionMessage> messages) {
    final List<SubscriptionCommitContext> commitContexts = new ArrayList<>();
    for (final SubscriptionMessage message : messages) {
      commitContexts.add(message.getCommitContext());
    }
    return commitContexts;
  }

  private void commit(final Iterable<SubscriptionCommitContext> commitContexts, final boolean nack)
      throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToSubscriptionCommitContexts =
        new HashMap<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      dataNodeIdToSubscriptionCommitContexts
          .computeIfAbsent(commitContext.getDataNodeId(), (id) -> new ArrayList<>())
          .add(commitContext);
    }
    int requestedCount = 0;
    int acceptedCount = 0;
    final List<SubscriptionCommitContext> failedCommitContexts = new ArrayList<>();
    for (final Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToSubscriptionCommitContexts.entrySet()) {
      final List<SubscriptionCommitContext> groupedCommitContexts = entry.getValue();
      requestedCount += groupedCommitContexts.size();
      final AbstractSubscriptionProvider.CommitResult commitResp =
          commitInternal(entry.getKey(), groupedCommitContexts, nack);
      final List<SubscriptionCommitContext> acceptedCommitContexts =
          commitResp.getAcceptedCommitContexts();
      acceptedCount += acceptedCommitContexts.size();
      if (!nack) {
        overlayCommittedPositions(commitResp.getCommittedProgressByTopic());
      }
      if (acceptedCommitContexts.size() != groupedCommitContexts.size()) {
        final List<SubscriptionCommitContext> failedInGroup =
            new ArrayList<>(groupedCommitContexts);
        acceptedCommitContexts.forEach(failedInGroup::remove);
        failedCommitContexts.addAll(failedInGroup);
      }
    }
    if (!failedCommitContexts.isEmpty()) {
      final String errorMessage =
          String.format(
              "%s commit (nack: %s) partially accepted, requested=%d, accepted=%d, failed=%d",
              this, nack, requestedCount, acceptedCount, failedCommitContexts.size());
      LOGGER.warn(
          SubscriptionMessages.LOG_ARG_FAILED_COMMIT_CONTEXTS_ARG_CF224D1E,
          errorMessage,
          failedCommitContexts);
      throw new SubscriptionRuntimeNonCriticalException(errorMessage);
    }
  }

  protected Set<SubscriptionMessage> ackWithPartialProgress(
      final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    final List<SubscriptionMessage> bufferedMessages = new ArrayList<>();
    final List<SubscriptionCommitContext> commitContexts = new ArrayList<>();
    for (final SubscriptionMessage message : messages) {
      bufferedMessages.add(message);
      commitContexts.add(message.getCommitContext());
    }

    final Set<SubscriptionCommitContext> removableCommitContexts =
        ackCommitContextsWithPartialProgress(commitContexts);
    final Set<SubscriptionMessage> removableMessages = new HashSet<>();
    for (final SubscriptionMessage message : bufferedMessages) {
      if (removableCommitContexts.contains(message.getCommitContext())) {
        removableMessages.add(message);
      }
    }
    return removableMessages;
  }

  protected Set<SubscriptionCommitContext> ackCommitContextsWithPartialProgress(
      final Iterable<SubscriptionCommitContext> commitContexts) throws SubscriptionException {
    final Map<Integer, List<SubscriptionCommitContext>> dataNodeIdToCommitContexts =
        new HashMap<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      dataNodeIdToCommitContexts
          .computeIfAbsent(commitContext.getDataNodeId(), ignored -> new ArrayList<>())
          .add(commitContext);
    }

    final Set<SubscriptionCommitContext> removableCommitContexts = new HashSet<>();
    for (final Entry<Integer, List<SubscriptionCommitContext>> entry :
        dataNodeIdToCommitContexts.entrySet()) {
      final List<SubscriptionCommitContext> groupedCommitContexts = entry.getValue();
      try {
        final AbstractSubscriptionProvider.CommitResult commitResp =
            commitInternal(entry.getKey(), groupedCommitContexts, false);
        overlayCommittedPositions(commitResp.getCommittedProgressByTopic());
        removableCommitContexts.addAll(commitResp.getAcceptedCommitContexts());
      } catch (final SubscriptionConnectionException e) {
        int stagedCount = 0;
        int retainedCount = 0;
        for (final SubscriptionCommitContext commitContext : groupedCommitContexts) {
          if (isConsensusCommitContext(commitContext)) {
            stagePendingRedirectAck(commitContext);
            removableCommitContexts.add(commitContext);
            stagedCount++;
          } else {
            retainedCount++;
          }
        }
        if (stagedCount > 0) {
          LOGGER.warn(
              SubscriptionMessages
                  .LOG_ARG_STAGED_ARG_CONSENSUS_ACK_S_REDIRECT_AFTER_PROVIDER_ARG_0F0C0623,
              this,
              stagedCount,
              entry.getKey());
        }
        if (retainedCount > 0) {
          LOGGER.warn(
              SubscriptionMessages
                  .LOG_ARG_KEEP_ARG_NON_CONSENSUS_ACK_S_PENDING_AFTER_PROVIDER_32F56904,
              this,
              retainedCount,
              entry.getKey(),
              e);
        }
      }
    }
    return removableCommitContexts;
  }

  protected void nack(final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
    final List<SubscriptionCommitContext> commitContexts = new ArrayList<>();
    for (final SubscriptionMessage message : messages) {
      // make every effort to delete stale intermediate file
      if (Objects.equals(SubscriptionMessageType.TS_FILE.getType(), message.getMessageType())
          &&
          // do not delete file that can resume from breakpoint
          !inFlightFilesCommitContextSet.contains(message.getCommitContext())) {
        try {
          message.getTsFile().deleteFile();
        } catch (final Exception ignored) {
        }
      }
      commitContexts.add(message.getCommitContext());
    }
    commit(commitContexts, true);
  }

  private void nack(final List<SubscriptionPollResponse> responses) throws SubscriptionException {
    final List<SubscriptionCommitContext> commitContexts = new ArrayList<>();
    for (final SubscriptionPollResponse response : responses) {
      // there is no stale intermediate file here
      commitContexts.add(response.getCommitContext());
    }
    commit(commitContexts, true);
  }

  private AbstractSubscriptionProvider.CommitResult commitInternal(
      final int dataNodeId,
      final List<SubscriptionCommitContext> subscriptionCommitContexts,
      final boolean nack)
      throws SubscriptionException {
    providers.acquireReadLock();
    try {
      final AbstractSubscriptionProvider provider = providers.getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        if (isClosed()) {
          return AbstractSubscriptionProvider.CommitResult.empty();
        }
        throw new SubscriptionConnectionException(
            String.format(
                SubscriptionMessages
                    .EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_COMMIT_NACK_ARG_MESSAGES_SUBSCRIPTION_PROVIDER_2026D7CE,
                this,
                nack,
                dataNodeId));
      }
      return provider.commit(subscriptionCommitContexts, nack);
    } finally {
      providers.releaseReadLock();
    }
  }

  /////////////////////////////// heartbeat ///////////////////////////////

  List<SubscriptionCommitContext> getProcessorBufferedCommitContexts(final int dataNodeId) {
    return Collections.emptyList();
  }

  private void submitHeartbeatWorker() {
    final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
    future[0] =
        SubscriptionExecutorServiceManager.submitHeartbeatWorker(
            () -> {
              if (isClosed()) {
                if (Objects.nonNull(future[0])) {
                  future[0].cancel(false);
                  LOGGER.info(SubscriptionMessages.CONSUMER_CANCEL_HEARTBEAT_WORKER, this);
                }
                return;
              }
              providers.heartbeat(this);
            },
            heartbeatIntervalMs);
    LOGGER.info(SubscriptionMessages.CONSUMER_SUBMIT_HEARTBEAT_WORKER, this);
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
                  LOGGER.info(SubscriptionMessages.CONSUMER_CANCEL_ENDPOINTS_SYNCER, this);
                }
                return;
              }
              providers.sync(this);
            },
            endpointsSyncIntervalMs);
    LOGGER.info(SubscriptionMessages.CONSUMER_SUBMIT_ENDPOINTS_SYNCER, this);
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
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SUBSCRIBE_TOPIC_ARG_06E872FE,
              this,
              topicNames));
    }
    for (final AbstractSubscriptionProvider provider : providers) {
      try {
        subscribedTopics = provider.subscribe(topicNames);
        return;
      } catch (final Exception e) {
        if (e instanceof SubscriptionOwnerFencedException) {
          throw (SubscriptionOwnerFencedException) e;
        }
        if (e instanceof SubscriptionPipeTimeoutException) {
          // degrade exception to log for pipe timeout
          LOGGER.warn(e.getMessage());
          return;
        }
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_SUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_4DF9FC46,
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
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_UNSUBSCRIBE_TOPIC_ARG_BF50B2B1,
              this,
              topicNames));
    }
    for (final AbstractSubscriptionProvider provider : providers) {
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
            SubscriptionMessages
                .LOG_ARG_FAILED_UNSUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_2205E8A8,
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

  /**
   * Sends seek request to ALL available providers. Unlike subscribe/unsubscribe, seek is only
   * considered successful if every available provider acknowledges it because data regions for the
   * topic may be distributed across different nodes.
   */
  private void seekOnAllProviders(
      final String topicName, final short seekType, final long timestamp)
      throws SubscriptionException {
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEK_TOPIC_ARG_9F93C2E7,
              this,
              topicName));
    }
    final List<AbstractSubscriptionProvider> failedProviders = new ArrayList<>();
    Throwable firstFailure = null;
    for (final AbstractSubscriptionProvider provider : providers) {
      try {
        provider.seek(topicName, seekType, timestamp);
      } catch (final Exception e) {
        failedProviders.add(provider);
        if (Objects.isNull(firstFailure)) {
          firstFailure = e;
        }
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_SEEK_TOPIC_ARG_SUBSCRIPTION_PROVIDER_ARG_SEEK_REQUIRES_15AFE61D,
            this,
            topicName,
            provider,
            e);
      }
    }
    if (!failedProviders.isEmpty()) {
      final String errorMessage =
          String.format(
              "%s failed to seek topic %s on subscription providers %s; seek requires every available provider to succeed",
              this, topicName, failedProviders);
      LOGGER.warn(errorMessage);
      throw new SubscriptionRuntimeCriticalException(errorMessage, firstFailure);
    }
  }

  /** Same all-provider success requirement as {@link #seekOnAllProviders(String, short, long)}. */
  private void seekToTopicProgressOnAllProviders(
      final String topicName, final TopicProgress topicProgress) throws SubscriptionException {
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEK_TOPIC_ARG_9F93C2E7,
              this,
              topicName));
    }
    final List<AbstractSubscriptionProvider> failedProviders = new ArrayList<>();
    Throwable firstFailure = null;
    for (final AbstractSubscriptionProvider provider : providers) {
      try {
        provider.seekToTopicProgress(topicName, topicProgress);
      } catch (final Exception e) {
        failedProviders.add(provider);
        if (Objects.isNull(firstFailure)) {
          firstFailure = e;
        }
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_SEEK_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_E999A05B,
            this,
            topicName,
            topicProgress.getRegionProgress().size(),
            provider,
            e);
      }
    }
    if (!failedProviders.isEmpty()) {
      final String errorMessage =
          String.format(
              "%s failed to seek topic %s to topicProgress(regionCount=%d) on subscription providers %s; seek requires every available provider to succeed",
              this, topicName, topicProgress.getRegionProgress().size(), failedProviders);
      LOGGER.warn(errorMessage);
      throw new SubscriptionRuntimeCriticalException(errorMessage, firstFailure);
    }
  }

  /** Same all-provider success requirement as {@link #seekOnAllProviders(String, short, long)}. */
  private void seekAfterTopicProgressOnAllProviders(
      final String topicName, final TopicProgress topicProgress) throws SubscriptionException {
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEKAFTER_TOPIC_ARG_C86D5F85,
              this,
              topicName));
    }
    final List<AbstractSubscriptionProvider> failedProviders = new ArrayList<>();
    Throwable firstFailure = null;
    for (final AbstractSubscriptionProvider provider : providers) {
      try {
        provider.seekAfterTopicProgress(topicName, topicProgress);
      } catch (final Exception e) {
        failedProviders.add(provider);
        if (Objects.isNull(firstFailure)) {
          firstFailure = e;
        }
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_SEEKAFTER_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_0C795E87,
            this,
            topicName,
            topicProgress.getRegionProgress().size(),
            provider,
            e);
      }
    }
    if (!failedProviders.isEmpty()) {
      final String errorMessage =
          String.format(
              "%s failed to seekAfter topic %s to topicProgress(regionCount=%d) on subscription providers %s; seek requires every available provider to succeed",
              this, topicName, topicProgress.getRegionProgress().size(), failedProviders);
      LOGGER.warn(errorMessage);
      throw new SubscriptionRuntimeCriticalException(errorMessage, firstFailure);
    }
  }

  private Map<String, TopicProgress> buildCurrentProgressByTopic(final Set<String> topicNames) {
    final Map<String, TopicProgress> result = new HashMap<>();
    for (final String topicName : topicNames) {
      final TopicProgress topicProgress = currentPositionsByTopic.get(topicName);
      if (Objects.isNull(topicProgress) || topicProgress.getRegionProgress().isEmpty()) {
        continue;
      }
      result.put(topicName, new TopicProgress(topicProgress.getRegionProgress()));
    }
    return result;
  }

  private void advanceCurrentPositions(final List<SubscriptionMessage> messages) {
    for (final SubscriptionMessage message : messages) {
      final SubscriptionCommitContext commitContext = message.getCommitContext();
      if (Objects.isNull(commitContext) || Objects.isNull(commitContext.getTopicName())) {
        continue;
      }
      mergeTopicProgress(
          currentPositionsByTopic,
          commitContext.getTopicName(),
          extractWriterId(commitContext),
          extractWriterProgress(commitContext));
    }
  }

  private boolean isConsensusCommitContext(final SubscriptionCommitContext commitContext) {
    return Objects.nonNull(commitContext)
        && Objects.nonNull(commitContext.getWriterId())
        && Objects.nonNull(commitContext.getWriterProgress())
        && Objects.nonNull(commitContext.getRegionId())
        && !commitContext.getRegionId().isEmpty();
  }

  private String buildTopicRegionKey(final SubscriptionCommitContext commitContext) {
    return commitContext.getTopicName() + '\u0001' + commitContext.getRegionId();
  }

  private void stagePendingRedirectAck(final SubscriptionCommitContext commitContext) {
    pendingRedirectAcksByTopicRegion
        .computeIfAbsent(
            buildTopicRegionKey(commitContext), ignored -> ConcurrentHashMap.newKeySet())
        .add(commitContext);
  }

  private void flushPendingRedirectAcks(final List<SubscriptionMessage> currentMessages) {
    final Map<String, Integer> redirectTargetByTopicRegion = new HashMap<>();
    for (final SubscriptionMessage message : currentMessages) {
      final SubscriptionCommitContext commitContext = message.getCommitContext();
      if (!isConsensusCommitContext(commitContext)) {
        continue;
      }
      redirectTargetByTopicRegion.put(
          buildTopicRegionKey(commitContext), commitContext.getDataNodeId());
    }

    for (final Entry<String, Integer> entry : redirectTargetByTopicRegion.entrySet()) {
      final Set<SubscriptionCommitContext> pendingContexts =
          pendingRedirectAcksByTopicRegion.get(entry.getKey());
      if (Objects.isNull(pendingContexts) || pendingContexts.isEmpty()) {
        continue;
      }

      final List<SubscriptionCommitContext> contextsToRedirect = new ArrayList<>(pendingContexts);
      try {
        final AbstractSubscriptionProvider.CommitResult commitResp =
            commitInternal(entry.getValue(), contextsToRedirect, false);
        overlayCommittedPositions(commitResp.getCommittedProgressByTopic());
        commitResp.getAcceptedCommitContexts().forEach(pendingContexts::remove);
        if (pendingContexts.isEmpty()) {
          pendingRedirectAcksByTopicRegion.remove(entry.getKey(), pendingContexts);
        }
      } catch (final SubscriptionException e) {
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_REDIRECT_ARG_PENDING_CONSENSUS_ACK_S_ARG_VIA_E6A10AC5,
            this,
            contextsToRedirect.size(),
            entry.getKey(),
            entry.getValue(),
            e);
      }
    }
  }

  private boolean isNewerWriterProgress(
      final long newPhysicalTime,
      final long newLocalSeq,
      final long oldPhysicalTime,
      final long oldLocalSeq) {
    return newPhysicalTime > oldPhysicalTime
        || (newPhysicalTime == oldPhysicalTime && newLocalSeq > oldLocalSeq);
  }

  private void clearCurrentPositions(final String topicName) {
    currentPositionsByTopic.remove(topicName);
  }

  private void clearCommittedPositions(final String topicName) {
    committedPositionsByTopic.remove(topicName);
  }

  private void clearPendingRedirectAcks(final String topicName) {
    final String prefix = topicName + '\u0001';
    pendingRedirectAcksByTopicRegion.keySet().removeIf(key -> key.startsWith(prefix));
  }

  private void setCurrentPositions(final String topicName, final TopicProgress topicProgress) {
    if (Objects.isNull(topicProgress) || topicProgress.getRegionProgress().isEmpty()) {
      currentPositionsByTopic.remove(topicName);
      return;
    }
    currentPositionsByTopic.put(topicName, new TopicProgress(topicProgress.getRegionProgress()));
  }

  private void setCommittedPositions(final String topicName, final TopicProgress topicProgress) {
    if (Objects.isNull(topicProgress) || topicProgress.getRegionProgress().isEmpty()) {
      committedPositionsByTopic.remove(topicName);
      return;
    }
    committedPositionsByTopic.put(topicName, new TopicProgress(topicProgress.getRegionProgress()));
  }

  private void overlayCurrentPositions(final String topicName, final TopicProgress topicProgress) {
    overlayTopicProgress(currentPositionsByTopic, topicName, topicProgress);
  }

  private void overlayCommittedPositions(
      final String topicName, final TopicProgress topicProgress) {
    overlayTopicProgress(committedPositionsByTopic, topicName, topicProgress);
  }

  private void overlayCommittedPositions(final Map<String, TopicProgress> progressByTopic) {
    if (Objects.isNull(progressByTopic) || progressByTopic.isEmpty()) {
      return;
    }
    progressByTopic.forEach(this::overlayCommittedPositions);
  }

  private void overlayTopicProgress(
      final Map<String, TopicProgress> progressByTopic,
      final String topicName,
      final TopicProgress topicProgress) {
    if (Objects.isNull(topicName)
        || topicName.isEmpty()
        || Objects.isNull(topicProgress)
        || topicProgress.getRegionProgress().isEmpty()) {
      return;
    }
    progressByTopic.compute(
        topicName,
        (ignored, oldTopicProgress) -> {
          final Map<String, RegionProgress> mergedRegionProgress =
              Objects.nonNull(oldTopicProgress)
                  ? new HashMap<>(oldTopicProgress.getRegionProgress())
                  : new HashMap<>();
          topicProgress
              .getRegionProgress()
              .forEach(
                  (regionId, regionProgress) -> {
                    if (Objects.isNull(regionId)
                        || regionId.isEmpty()
                        || Objects.isNull(regionProgress)
                        || regionProgress.getWriterPositions().isEmpty()) {
                      return;
                    }
                    mergedRegionProgress.put(
                        regionId,
                        new RegionProgress(new HashMap<>(regionProgress.getWriterPositions())));
                  });
          return mergedRegionProgress.isEmpty() ? null : new TopicProgress(mergedRegionProgress);
        });
  }

  private WriterId extractWriterId(final SubscriptionCommitContext commitContext) {
    if (Objects.nonNull(commitContext.getWriterId())) {
      return commitContext.getWriterId();
    }
    if (Objects.isNull(commitContext.getRegionId()) || commitContext.getRegionId().isEmpty()) {
      return null;
    }
    return new WriterId(commitContext.getRegionId(), commitContext.getDataNodeId());
  }

  private WriterProgress extractWriterProgress(final SubscriptionCommitContext commitContext) {
    if (Objects.nonNull(commitContext.getWriterProgress())) {
      return commitContext.getWriterProgress();
    }
    if (commitContext.getLocalSeq() < 0) {
      return null;
    }
    return new WriterProgress(commitContext.getPhysicalTime(), commitContext.getLocalSeq());
  }

  private void mergeTopicProgress(
      final Map<String, TopicProgress> progressByTopic,
      final String topicName,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    if (Objects.isNull(writerId)
        || Objects.isNull(writerProgress)
        || Objects.isNull(topicName)
        || topicName.isEmpty()) {
      return;
    }
    progressByTopic.compute(
        topicName,
        (key, oldTopicProgress) -> {
          final Map<String, RegionProgress> regionProgressById =
              Objects.nonNull(oldTopicProgress)
                  ? new HashMap<>(oldTopicProgress.getRegionProgress())
                  : new HashMap<>();
          final RegionProgress oldRegionProgress = regionProgressById.get(writerId.getRegionId());
          final Map<WriterId, WriterProgress> writerPositions =
              Objects.nonNull(oldRegionProgress)
                  ? new HashMap<>(oldRegionProgress.getWriterPositions())
                  : new HashMap<>();
          writerPositions.merge(
              writerId,
              writerProgress,
              (oldVal, newVal) ->
                  isNewerWriterProgress(
                          newVal.getPhysicalTime(),
                          newVal.getLocalSeq(),
                          oldVal.getPhysicalTime(),
                          oldVal.getLocalSeq())
                      ? newVal
                      : oldVal);
          regionProgressById.put(writerId.getRegionId(), new RegionProgress(writerPositions));
          return new TopicProgress(regionProgressById);
        });
  }

  Map<Integer, TEndPoint> fetchAllEndPointsWithRedirection() throws SubscriptionException {
    final List<AbstractSubscriptionProvider> providers = this.providers.getAllAvailableProviders();
    if (providers.isEmpty()) {
      throw new SubscriptionConnectionException(
          String.format(
              SubscriptionMessages
                  .EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_FETCH_ALL_ENDPOINTS_D232693E,
              this));
    }
    for (final AbstractSubscriptionProvider provider : providers) {
      try {
        return provider.heartbeat().getEndPoints();
      } catch (final Exception e) {
        LOGGER.warn(
            SubscriptionMessages
                .LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_25651CAD,
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

  /////////////////////////////// stringify ///////////////////////////////

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("consumerId", consumerId);
    result.put("consumerGroupId", consumerGroupId);
    result.put("ownerId", ownerId);
    result.put("ownerEpoch", String.valueOf(ownerEpoch));
    result.put("isClosed", isClosed.toString());
    result.put("fileSaveDir", fileSaveDir);
    result.put(
        "inFlightFilesCommitContextSet",
        CollectionUtils.getLimitedString(inFlightFilesCommitContextSet, 32));
    result.put(
        "subscribedTopicNames", CollectionUtils.getLimitedString(subscribedTopics.keySet(), 32));
    return result;
  }

  protected Map<String, String> allReportMessage() {
    final Map<String, String> result = new HashMap<>();
    result.put("consumerId", consumerId);
    result.put("consumerGroupId", consumerGroupId);
    result.put("ownerId", ownerId);
    result.put("ownerEpoch", String.valueOf(ownerEpoch));
    result.put("heartbeatIntervalMs", String.valueOf(heartbeatIntervalMs));
    result.put("endpointsSyncIntervalMs", String.valueOf(endpointsSyncIntervalMs));
    result.put("providers", providers.toString());
    result.put("isClosed", isClosed.toString());
    result.put("isReleased", isReleased.toString());
    result.put("fileSaveDir", fileSaveDir);
    result.put("fileSaveFsync", String.valueOf(fileSaveFsync));
    result.put("inFlightFilesCommitContextSet", inFlightFilesCommitContextSet.toString());
    result.put("thriftMaxFrameSize", String.valueOf(thriftMaxFrameSize));
    result.put("connectionTimeoutInMs", String.valueOf(connectionTimeoutInMs));
    result.put("maxPollParallelism", String.valueOf(maxPollParallelism));
    result.put("subscribedTopics", subscribedTopics.toString());
    return result;
  }
}
