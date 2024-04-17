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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.common.PollMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.PollTsFileMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPollMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TabletsMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileInfoMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFilePieceMessagePayload;
import org.apache.iotdb.rpc.subscription.payload.common.TsFileSealMessagePayload;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionPullConsumer extends SubscriptionConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPullConsumer.class);

  private final boolean autoCommit;
  private final long autoCommitIntervalMs;

  private ScheduledExecutorService autoCommitWorkerExecutor;
  private SortedMap<Long, Set<SubscriptionMessage>> uncommittedMessages;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  @Override
  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// ctor ///////////////////////////////

  public SubscriptionPullConsumer(SubscriptionPullConsumer.Builder builder) {
    super(builder);

    this.autoCommit = builder.autoCommit;
    this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
  }

  public SubscriptionPullConsumer(Properties properties) {
    this(
        properties,
        (Boolean)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_KEY, ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE),
        (Long)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_KEY,
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE));
  }

  private SubscriptionPullConsumer(
      Properties properties, boolean autoCommit, long autoCommitIntervalMs) {
    super(
        new Builder().autoCommit(autoCommit).autoCommitIntervalMs(autoCommitIntervalMs),
        properties);

    this.autoCommit = autoCommit;
    this.autoCommitIntervalMs = autoCommitIntervalMs;
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();

    if (autoCommit) {
      launchAutoCommitWorker();
    }

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      if (autoCommit) {
        // shutdown auto commit worker
        shutdownAutoCommitWorker();

        // commit all uncommitted messages
        commitAllUncommittedMessages();
      }
      super.close();
    } finally {
      isClosed.set(true);
    }
  }

  /////////////////////////////// poll & commit ///////////////////////////////

  public List<SubscriptionMessage> poll(Duration timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(Collections.emptySet(), timeoutMs.toMillis());
  }

  public List<SubscriptionMessage> poll(long timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(Collections.emptySet(), timeoutMs);
  }

  public List<SubscriptionMessage> poll(Set<String> topicNames, Duration timeoutMs)
      throws TException, IOException, StatementExecutionException {
    return poll(topicNames, timeoutMs.toMillis());
  }

  public List<SubscriptionMessage> poll(Set<String> topicNames, long timeoutMs)
      throws TException, IOException, StatementExecutionException {
    List<SubscriptionPolledMessage> polledMessages = new ArrayList<>();

    acquireReadLock();
    try {
      for (final SubscriptionProvider provider : getAllAvailableProviders()) {
        // TODO: network timeout
        polledMessages.addAll(
            provider
                .getSessionConnection()
                .poll(
                    new SubscriptionPollMessage(
                        SubscriptionPollMessageType.POLL.getType(),
                        new PollMessagePayload(topicNames),
                        timeoutMs)));
      }
    } finally {
      releaseReadLock();
    }

    final List<SubscriptionMessage> messages = new ArrayList<>();
    for (final SubscriptionPolledMessage polledMessage : polledMessages) {
      final short messageType = polledMessage.getMessageType();
      if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPolledMessageType.valueOf(messageType)) {
          case TABLETS:
            messages.add(
                new SubscriptionMessage(
                    polledMessage.getCommitContext(),
                    ((TabletsMessagePayload) polledMessage.getMessagePayload()).getTablets()));
            break;
          case TS_FILE_INFO:
            try {
              final SubscriptionMessage message =
                  pollTsFile(
                      polledMessage.getCommitContext(),
                      ((TsFileInfoMessagePayload) polledMessage.getMessagePayload()).getFileName(),
                      timeoutMs);
              if (Objects.isNull(message)) {
                throw new Exception("poll empty tsfile, will retry later...");
              }
              messages.add(message);
            } catch (Exception e) {
              LOGGER.warn(e.getMessage());
            }
            break;
          default:
            LOGGER.warn("unexpected message type: {}", messageType);
            break;
        }
      } else {
        LOGGER.warn("unexpected message type: {}", messageType);
      }
    }

    if (autoCommit) {
      long currentTimestamp = System.currentTimeMillis();
      long index = currentTimestamp / autoCommitIntervalMs;
      if (currentTimestamp % autoCommitIntervalMs == 0) {
        index -= 1;
      }
      uncommittedMessages
          .computeIfAbsent(index, o -> new ConcurrentSkipListSet<>())
          .addAll(messages);
    }

    return messages;
  }

  private SubscriptionMessage pollTsFile(
      SubscriptionCommitContext commitContext, String fileName, long timeoutMs)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    final int dataNodeId = commitContext.getDataNodeId();
    final String topicName = commitContext.getTopicName();

    final Path filePath = getTsFileDir(topicName).resolve(fileName);
    Files.createFile(filePath);
    final File file = filePath.toFile();
    final RandomAccessFile fileWriter = new RandomAccessFile(file, "rw");
    commitContextToTsFile.put(commitContext, new Pair<>(file, fileWriter));

    LOGGER.info("{} start poll tsfile: {}", this, file.getAbsolutePath());

    long endWritingOffset = 0;
    while (true) {
      final List<SubscriptionPolledMessage> polledMessages =
          pollTsFileInternal(dataNodeId, topicName, fileName, endWritingOffset, timeoutMs);
      if (Objects.isNull(polledMessages) || polledMessages.size() != 1) {
        return null;
      }
      final SubscriptionPolledMessage polledMessage = polledMessages.get(0);
      if (Objects.isNull(polledMessage)) {
        return null;
      }
      final short messageType = polledMessage.getMessageType();
      if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
        switch (SubscriptionPolledMessageType.valueOf(messageType)) {
          case TS_FILE_PIECE:
            {
              final TsFilePieceMessagePayload messagePayload =
                  (TsFilePieceMessagePayload) polledMessage.getMessagePayload();
              if (Objects.isNull(messagePayload)) {
                return null;
              }
              // check file name
              if (!fileName.equals(messagePayload.getFileName())) {
                return null;
              }
              // write file piece
              fileWriter.write(messagePayload.getFilePiece());
              fileWriter.getFD().sync();
              // update offset
              endWritingOffset = messagePayload.getEndWritingOffset();
              break;
            }
          case TS_FILE_SEAL:
            {
              final TsFileSealMessagePayload messagePayload =
                  (TsFileSealMessagePayload) polledMessage.getMessagePayload();
              if (Objects.isNull(messagePayload)) {
                return null;
              }
              // check file name
              if (!fileName.equals(messagePayload.getFileName())) {
                return null;
              }
              // check file length
              if (fileWriter.length() != messagePayload.getFileLength()) {
                return null;
              }
              // sync and close
              fileWriter.getFD().sync();
              fileWriter.close();
              commitContextToTsFile.remove(commitContext);
              LOGGER.info("{} successfully poll tsfile: {}", this, file.getAbsolutePath());
              // generate subscription message
              return new SubscriptionMessage(commitContext, file.getAbsolutePath());
            }
          default:
            LOGGER.warn("unexpected message type: {}", messageType);
            return null;
        }
      } else {
        LOGGER.warn("unexpected message type: {}", messageType);
        return null;
      }
    }
  }

  private List<SubscriptionPolledMessage> pollTsFileInternal(
      int dataNodeId, String topicName, String fileName, long endWritingOffset, long timeoutMs)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      final SubscriptionProvider provider = getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new IoTDBConnectionException(
            String.format(
                "something unexpected happened when poll tsfile from subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                dataNodeId));
      }
      return provider
          .getSessionConnection()
          .poll(
              new SubscriptionPollMessage(
                  SubscriptionPollMessageType.POLL_TS_FILE.getType(),
                  new PollTsFileMessagePayload(topicName, fileName, endWritingOffset),
                  timeoutMs));
    } finally {
      releaseReadLock();
    }
  }

  public void commitSync(SubscriptionMessage message)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    commitSync(Collections.singletonList(message));
  }

  public void commitSync(Iterable<SubscriptionMessage> messages)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
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

  /////////////////////////////// utility ///////////////////////////////

  private void commitSyncInternal(
      int dataNodeId, List<SubscriptionCommitContext> subscriptionCommitContexts)
      throws TException, IOException, StatementExecutionException, IoTDBConnectionException {
    acquireReadLock();
    try {
      final SubscriptionProvider provider = getProvider(dataNodeId);
      if (Objects.isNull(provider) || !provider.isAvailable()) {
        throw new IoTDBConnectionException(
            String.format(
                "something unexpected happened when commit messages to subscription provider with data node id %s, the subscription provider may be unavailable or not existed",
                dataNodeId));
      }
      provider.getSessionConnection().commitSync(subscriptionCommitContexts);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////// auto commit ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchAutoCommitWorker() {
    uncommittedMessages = new ConcurrentSkipListMap<>();
    autoCommitWorkerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(
                      Thread.currentThread().getThreadGroup(),
                      r,
                      "PullConsumerAutoCommitWorker",
                      0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    autoCommitWorkerExecutor.scheduleAtFixedRate(
        new PullConsumerAutoCommitWorker(this), 0, autoCommitIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void shutdownAutoCommitWorker() {
    autoCommitWorkerExecutor.shutdown();
    autoCommitWorkerExecutor = null;
  }

  private void commitAllUncommittedMessages() {
    for (Map.Entry<Long, Set<SubscriptionMessage>> entry : uncommittedMessages.entrySet()) {
      try {
        commitSync(entry.getValue());
        uncommittedMessages.remove(entry.getKey());
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when commit messages during close", e);
      }
    }
  }

  long getAutoCommitIntervalMs() {
    return autoCommitIntervalMs;
  }

  SortedMap<Long, Set<SubscriptionMessage>> getUncommittedMessages() {
    return uncommittedMessages;
  }

  /////////////////////////////// builder ///////////////////////////////

  public static class Builder extends SubscriptionConsumer.Builder {

    private boolean autoCommit = ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE;
    private long autoCommitIntervalMs = ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE;

    public Builder host(String host) {
      super.host(host);
      return this;
    }

    public Builder port(int port) {
      super.port(port);
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      super.nodeUrls(nodeUrls);
      return this;
    }

    public Builder username(String username) {
      super.username(username);
      return this;
    }

    public Builder password(String password) {
      super.password(password);
      return this;
    }

    public Builder consumerId(String consumerId) {
      super.consumerId(consumerId);
      return this;
    }

    public Builder consumerGroupId(String consumerGroupId) {
      super.consumerGroupId(consumerGroupId);
      return this;
    }

    public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
      super.heartbeatIntervalMs(heartbeatIntervalMs);
      return this;
    }

    public Builder endpointsSyncIntervalMs(long endpointsSyncIntervalMs) {
      super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
      return this;
    }

    public Builder autoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder autoCommitIntervalMs(long autoCommitIntervalMs) {
      this.autoCommitIntervalMs =
          Math.max(autoCommitIntervalMs, ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    @Override
    public SubscriptionPullConsumer buildPullConsumer() {
      return new SubscriptionPullConsumer(this);
    }

    @Override
    public SubscriptionPushConsumer buildPushConsumer() {
      throw new SubscriptionException(
          "SubscriptionPullConsumer.Builder do not support build push consumer.");
    }
  }
}
