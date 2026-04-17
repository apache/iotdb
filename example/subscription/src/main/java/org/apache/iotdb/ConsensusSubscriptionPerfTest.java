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

package org.apache.iotdb;

import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.PollResult;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

/**
 * Manual performance test for consensus subscription.
 *
 * <p>Typical usage:
 *
 * <pre>
 *   java ... org.apache.iotdb.ConsensusSubscriptionPerfTest
 *   java ... org.apache.iotdb.ConsensusSubscriptionPerfTest --topic=topic_perf --group=cg_perf
 *   java ... org.apache.iotdb.ConsensusSubscriptionPerfTest --path=root.db_bench.**
 *   java ... org.apache.iotdb.ConsensusSubscriptionPerfTest --orderMode=per-writer
 *   java ... org.apache.iotdb.ConsensusSubscriptionPerfTest --topic=topic_perf --createTopicOnly=true
 * </pre>
 *
 * <p>This tool is designed to be started before a benchmark writer (for example iot-benchmark). It
 * creates a consensus-mode topic by default and continuously prints subscription throughput
 * statistics.
 */
public class ConsensusSubscriptionPerfTest {

  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
  private static final long RANDOM_SEEK_CHECKPOINT_INTERVAL_ROWS = 100_000L;

  public static void main(final String[] args) throws Exception {
    final PerfConfig config = PerfConfig.parse(args);

    if (config.help) {
      printUsage();
      return;
    }

    System.out.println("=== Consensus Subscription Performance Test ===");
    System.out.println(config);

    if (config.autoCreateTopic) {
      createTopicIfNeeded(config);
    }

    if (config.createTopicOnly) {
      System.out.println(
          String.format(
              Locale.ROOT,
              "[%s] Topic is ready. Exiting due to createTopicOnly=true",
              nowString()));
      return;
    }

    final PerfStats stats = new PerfStats(config.enableEquivalentRowTracking());
    final RandomSeekController randomSeekController = new RandomSeekController(config.randomSeek);
    final ScheduledSeekController scheduledSeekController =
        new ScheduledSeekController(config.seekCaptureRows > 0 && config.seekTriggerNanos > 0);
    final ConsumerRestartController consumerRestartController =
        new ConsumerRestartController(config.consumerStopNanos > 0);
    final ConsumerPauseController consumerPauseController =
        new ConsumerPauseController(config.consumerPauseEveryRows);
    long startNanoTime;
    long lastReportNanoTime;
    final Snapshot[] lastSnapshot = new Snapshot[1];
    final ProcessingRateLimiter processingRateLimiter =
        new ProcessingRateLimiter(config.targetPointsPerSec);
    SubscriptionTreePullConsumer consumer = null;
    PollResult lastPollResult = emptyPollResult(stats);

    try {
      consumer = openAndSubscribeConsumer(config);

      System.out.println(
          String.format(
              Locale.ROOT, "[%s] Subscribed. Waiting for benchmark writes...", nowString()));

      if (config.waitBeforePollNanos > 0) {
        System.out.println(
            String.format(
                Locale.ROOT,
                "[%s] Delaying poll start for %.3f second(s)...",
                nowString(),
                config.waitBeforePollSec));
        LockSupport.parkNanos(config.waitBeforePollNanos);
      }

      System.out.println(String.format(Locale.ROOT, "[%s] Starting poll loop.", nowString()));

      startNanoTime = System.nanoTime();
      lastReportNanoTime = startNanoTime;
      lastSnapshot[0] = Snapshot.capture(stats);

      while (config.durationSec <= 0
          || nanosToSeconds(System.nanoTime() - startNanoTime) < config.durationSec) {
        final long loopNowNanoTime = System.nanoTime();
        final long elapsedNanoTime = loopNowNanoTime - startNanoTime;

        if (shouldStopConsumer(config, consumerRestartController, elapsedNanoTime)
            && Objects.nonNull(consumer)) {
          consumerRestartController.stopPerformed = true;
          consumerRestartController.stoppedNanoTime = System.nanoTime();
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "[%s] Consumer polling paused at elapsedSec=%.3f; polling will resume at %.3f second(s).",
                  nowString(),
                  elapsedNanoTime / 1_000_000_000.0d,
                  config.consumerResumeSec));
        }

        if (shouldPauseConsumerByRows(config, consumerPauseController, stats.totalRows)
            && Objects.nonNull(consumer)) {
          consumerPauseController.pausePerformedCount++;
          consumerPauseController.paused = true;
          consumerPauseController.stoppedNanoTime = System.nanoTime();
          consumerPauseController.nextPauseRows = stats.totalRows + config.consumerPauseEveryRows;
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "[%s] Consumer paused after rows=%d; polling will resume in %.3f second(s).",
                  nowString(),
                  stats.totalRows,
                  config.consumerPauseDurationSec));
        }

        if (shouldResumeConsumer(config, consumerRestartController, elapsedNanoTime)
            && Objects.nonNull(consumer)) {
          final long resumedNanoTime = System.nanoTime();
          processingRateLimiter.pauseForDowntime(
              resumedNanoTime - consumerRestartController.stoppedNanoTime);
          consumerRestartController.resumePerformed = true;
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "[%s] Consumer polling resumed at elapsedSec=%.3f after downtimeSec=%.3f.",
                  nowString(),
                  (resumedNanoTime - startNanoTime) / 1_000_000_000.0d,
                  (resumedNanoTime - consumerRestartController.stoppedNanoTime)
                      / 1_000_000_000.0d));
        }

        if (shouldResumeConsumerByRows(config, consumerPauseController)
            && Objects.nonNull(consumer)) {
          final long resumedNanoTime = System.nanoTime();
          processingRateLimiter.pauseForDowntime(
              resumedNanoTime - consumerPauseController.stoppedNanoTime);
          consumerPauseController.paused = false;
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "[%s] Consumer resumed after row-based pause at rows=%d, downtimeSec=%.3f.",
                  nowString(),
                  stats.totalRows,
                  (resumedNanoTime - consumerPauseController.stoppedNanoTime) / 1_000_000_000.0d));
        }

        final boolean pollingPaused =
            consumerRestartController.enabled
                    && consumerRestartController.stopPerformed
                    && !consumerRestartController.resumePerformed
                || consumerPauseController.enabled && consumerPauseController.paused;

        final PollResult pollResult;
        if (Objects.nonNull(consumer) && !pollingPaused) {
          pollResult = consumer.pollWithInfo(config.pollTimeoutMs);
          handlePollResult(
              pollResult,
              stats,
              config.processDelayNanos,
              processingRateLimiter,
              config.ingestWallTimeSensor);
          captureScheduledSeekCheckpoint(consumer, config, stats, scheduledSeekController);
          captureRandomSeekCheckpoint(consumer, config, stats, randomSeekController);
          maybePerformScheduledSeek(
              consumer, config, stats, scheduledSeekController, System.nanoTime() - startNanoTime);
          maybePerformRandomSeek(consumer, config, stats, randomSeekController);
        } else {
          LockSupport.parkNanos(Math.min(100_000_000L, config.pollTimeoutMs * 1_000_000L));
          pollResult = emptyPollResult(stats);
        }
        lastPollResult = pollResult;

        final long nowNanoTime = System.nanoTime();
        if (nowNanoTime - lastReportNanoTime >= config.reportIntervalSec * 1_000_000_000L) {
          printReport(
              "interval",
              lastSnapshot[0],
              Snapshot.capture(stats),
              nowNanoTime - lastReportNanoTime,
              pollResult);
          lastSnapshot[0] = Snapshot.capture(stats);
          lastReportNanoTime = nowNanoTime;
        }
      }

      printReport(
          "final",
          Snapshot.zero(),
          Snapshot.capture(stats),
          System.nanoTime() - startNanoTime,
          lastPollResult);
    } finally {
      if (Objects.nonNull(consumer)) {
        consumer.close();
      }
    }
  }

  private static void createTopicIfNeeded(final PerfConfig config) throws Exception {
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder()
            .host(config.host)
            .port(config.port)
            .username(config.username)
            .password(config.password)
            .build()) {
      session.open();

      final Properties topicConfig = new Properties();
      topicConfig.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      topicConfig.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_RECORD_HANDLER_VALUE);
      topicConfig.put(TopicConstant.PATH_KEY, config.path);
      topicConfig.put(TopicConstant.ORDER_MODE_KEY, config.orderMode);
      session.createTopicIfNotExists(config.topic, topicConfig);
    }
  }

  private static SubscriptionTreePullConsumer createConsumer(final PerfConfig config) {
    return (SubscriptionTreePullConsumer)
        new SubscriptionTreePullConsumerBuilder()
            .host(config.host)
            .port(config.port)
            .username(config.username)
            .password(config.password)
            .consumerId(config.consumer)
            .consumerGroupId(config.group)
            .autoCommit(config.autoCommit)
            .autoCommitIntervalMs(config.autoCommitIntervalMs)
            .maxPollParallelism(1)
            .build();
  }

  private static SubscriptionTreePullConsumer openAndSubscribeConsumer(final PerfConfig config)
      throws Exception {
    final SubscriptionTreePullConsumer consumer = createConsumer(config);
    consumer.open();
    consumer.subscribe(config.topic);
    return consumer;
  }

  private static PollResult emptyPollResult(final PerfStats stats) {
    return new PollResult(Collections.<SubscriptionMessage>emptyList(), 0, stats.lastWatermark);
  }

  private static boolean shouldStopConsumer(
      final PerfConfig config,
      final ConsumerRestartController controller,
      final long elapsedNanoTime) {
    return controller.enabled
        && !controller.stopPerformed
        && elapsedNanoTime >= config.consumerStopNanos;
  }

  private static boolean shouldResumeConsumer(
      final PerfConfig config,
      final ConsumerRestartController controller,
      final long elapsedNanoTime) {
    return controller.enabled
        && controller.stopPerformed
        && !controller.resumePerformed
        && elapsedNanoTime >= config.consumerResumeNanos;
  }

  private static boolean shouldPauseConsumerByRows(
      final PerfConfig config, final ConsumerPauseController controller, final long totalRows) {
    return controller.enabled
        && !controller.paused
        && totalRows > 0
        && totalRows >= controller.nextPauseRows
        && config.consumerPauseEveryRows > 0;
  }

  private static boolean shouldResumeConsumerByRows(
      final PerfConfig config, final ConsumerPauseController controller) {
    return controller.enabled
        && controller.paused
        && controller.stoppedNanoTime > 0
        && System.nanoTime() - controller.stoppedNanoTime >= config.consumerPauseDurationNanos;
  }

  private static void handlePollResult(
      final PollResult pollResult,
      final PerfStats stats,
      final long processDelayNanos,
      final ProcessingRateLimiter processingRateLimiter,
      final String ingestWallTimeSensor) {
    stats.totalPollCalls++;
    stats.lastBufferedCount = pollResult.getBufferedCount();
    if (pollResult.getWatermark() >= 0) {
      stats.lastWatermark = pollResult.getWatermark();
    }

    final List<SubscriptionMessage> messages = pollResult.getMessages();
    if (messages.isEmpty()) {
      stats.emptyPollCalls++;
      return;
    }

    for (final SubscriptionMessage message : messages) {
      stats.totalMessages++;

      if (message.getMessageType() == SubscriptionMessageType.WATERMARK.getType()) {
        stats.totalWatermarkMessages++;
        if (message.getWatermarkTimestamp() >= 0) {
          stats.lastWatermark = Math.max(stats.lastWatermark, message.getWatermarkTimestamp());
        }
        continue;
      }

      if (message.getMessageType() == SubscriptionMessageType.TS_FILE.getType()) {
        stats.totalTsFileMessages++;
        maybeApplyProcessingDelay(processDelayNanos, processingRateLimiter, 0);
        continue;
      }

      if (message.getMessageType() == SubscriptionMessageType.RECORD_HANDLER.getType()) {
        final Iterator<Tablet> tabletIterator = message.getRecordTabletIterator();
        while (tabletIterator.hasNext()) {
          final Tablet tablet = tabletIterator.next();
          stats.totalTablets++;
          final int rowSize = tablet.getRowSize();
          stats.totalRows += rowSize;
          stats.totalApproxBytes += tablet.ramBytesUsed();
          updateOrderingStats(stats, tablet, rowSize);
          updateLatencyStats(stats, tablet, rowSize, ingestWallTimeSensor);
          maybeApplyProcessingDelay(
              processDelayNanos, processingRateLimiter, estimateTabletPoints(tablet, rowSize));
        }
      }
    }
  }

  private static long estimateTabletPoints(final Tablet tablet, final int rowSize) {
    if (rowSize <= 0) {
      return 0L;
    }
    return (long) rowSize * tablet.getSchemas().size();
  }

  private static void updateOrderingStats(
      final PerfStats stats, final Tablet tablet, final int rowSize) {
    if (rowSize <= 0) {
      return;
    }

    final String deviceId = Objects.toString(tablet.getDeviceId(), "<unknown-device>");
    long lastSeenTimestamp = stats.lastSeenTimestampByDevice.getOrDefault(deviceId, Long.MIN_VALUE);

    for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
      final long currentTimestamp = tablet.getTimestamp(rowIndex);
      if (stats.equivalentRowTracker == null
          || stats.equivalentRowTracker.record(deviceId, currentTimestamp)) {
        stats.totalEquivalentRows++;
      }
      if (lastSeenTimestamp != Long.MIN_VALUE && currentTimestamp < lastSeenTimestamp) {
        stats.totalOutOfOrderRows++;
        final long regression = lastSeenTimestamp - currentTimestamp;
        if (regression > stats.maxTimestampRegression) {
          stats.maxTimestampRegression = regression;
        }
      }
      if (currentTimestamp > lastSeenTimestamp) {
        lastSeenTimestamp = currentTimestamp;
      }
    }

    stats.lastSeenTimestampByDevice.put(deviceId, lastSeenTimestamp);
  }

  private static void updateLatencyStats(
      final PerfStats stats,
      final Tablet tablet,
      final int rowSize,
      final String ingestWallTimeSensor) {
    if (rowSize <= 0 || ingestWallTimeSensor == null || ingestWallTimeSensor.isEmpty()) {
      return;
    }

    final int sensorIndex = findMeasurementIndex(tablet, ingestWallTimeSensor);
    if (sensorIndex < 0) {
      return;
    }

    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    if (sensorIndex >= schemas.size()
        || schemas.get(sensorIndex).getType() != TSDataType.INT64
        || sensorIndex >= tablet.getValues().length
        || !(tablet.getValues()[sensorIndex] instanceof long[])) {
      return;
    }

    final long[] ingestWallTimes = (long[]) tablet.getValues()[sensorIndex];
    final BitMap[] bitMaps = tablet.getBitMaps();
    final BitMap bitMap =
        bitMaps != null && sensorIndex < bitMaps.length ? bitMaps[sensorIndex] : null;
    final long nowMs = System.currentTimeMillis();

    for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
      if (bitMap != null && bitMap.isMarked(rowIndex)) {
        continue;
      }

      final long ingestWallTimeMs = ingestWallTimes[rowIndex];
      final long latencyMs = Math.max(0L, nowMs - ingestWallTimeMs);
      stats.recordLatency(latencyMs);
    }
  }

  private static int findMeasurementIndex(final Tablet tablet, final String measurementName) {
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    for (int i = 0, size = schemas.size(); i < size; i++) {
      if (measurementName.equals(schemas.get(i).getMeasurementName())) {
        return i;
      }
    }
    return -1;
  }

  private static void maybeApplyProcessingDelay(
      final long processDelayNanos,
      final ProcessingRateLimiter processingRateLimiter,
      final long processedPoints) {
    if (processingRateLimiter.isEnabled()) {
      processingRateLimiter.acquire(processedPoints);
      return;
    }
    if (processDelayNanos > 0) {
      LockSupport.parkNanos(processDelayNanos);
    }
  }

  private static void captureRandomSeekCheckpoint(
      final SubscriptionTreePullConsumer consumer,
      final PerfConfig config,
      final PerfStats stats,
      final RandomSeekController controller)
      throws Exception {
    if (!controller.enabled
        || stats.totalRows <= 0
        || (controller.lastCapturedRows >= 0
            && stats.totalRows - controller.lastCapturedRows
                < RANDOM_SEEK_CHECKPOINT_INTERVAL_ROWS)) {
      return;
    }

    TopicProgress progress = consumer.committedPositions(config.topic);
    String source = "committed";
    if (isEmptyTopicProgress(progress)) {
      progress = consumer.positions(config.topic);
      source = "current";
    }
    if (isEmptyTopicProgress(progress)) {
      return;
    }

    final TopicProgress safeProgress = new TopicProgress(progress.getRegionProgress());
    if (Objects.equals(controller.lastCapturedProgress, safeProgress)) {
      controller.lastCapturedRows = stats.totalRows;
      return;
    }

    controller.checkpoints.add(
        new SeekCheckpoint(stats.totalRows, stats.totalEquivalentRows, source, safeProgress));
    controller.lastCapturedRows = stats.totalRows;
    controller.lastCapturedProgress = safeProgress;
    stats.totalRandomSeekCheckpoints = controller.checkpoints.size();
  }

  private static void captureScheduledSeekCheckpoint(
      final SubscriptionTreePullConsumer consumer,
      final PerfConfig config,
      final PerfStats stats,
      final ScheduledSeekController controller)
      throws Exception {
    if (!controller.enabled || Objects.nonNull(controller.checkpoint)) {
      return;
    }
    if (stats.totalRows < config.seekCaptureRows) {
      return;
    }

    final TopicProgress currentProgress = consumer.positions(config.topic);
    if (isEmptyTopicProgress(currentProgress)) {
      return;
    }
    final TopicProgress committedProgress = consumer.committedPositions(config.topic);

    controller.checkpoint =
        new SeekCheckpoint(
            stats.totalRows,
            stats.totalEquivalentRows,
            "current",
            new TopicProgress(currentProgress.getRegionProgress()));

    System.out.println(
        String.format(
            Locale.ROOT,
            "[%s] Scheduled seek checkpoint captured: checkpointRows=%d, checkpointEquivalentRows=%d, progressSource=current, triggerSec=%.3f",
            nowString(),
            controller.checkpoint.rawRows,
            controller.checkpoint.equivalentRows,
            config.seekTriggerSec));
  }

  private static void maybePerformRandomSeek(
      final SubscriptionTreePullConsumer consumer,
      final PerfConfig config,
      final PerfStats stats,
      final RandomSeekController controller)
      throws Exception {
    if (!controller.enabled
        || controller.performed
        || stats.totalRows < config.randomSeekMinRows
        || controller.checkpoints.size() < 2) {
      return;
    }

    final int candidateCount = controller.checkpoints.size() - 1;
    final SeekCheckpoint targetCheckpoint =
        controller.checkpoints.get(controller.random.nextInt(candidateCount));

    consumer.seekAfter(config.topic, targetCheckpoint.topicProgress);

    controller.performed = true;
    stats.totalRandomSeeks++;
    stats.lastRandomSeekSourceRows = targetCheckpoint.rawRows;
    stats.lastRandomSeekEquivalentRows = targetCheckpoint.equivalentRows;
    stats.lastRandomSeekObservedRows = stats.totalRows;

    System.out.println(
        String.format(
            Locale.ROOT,
            "[%s] Random seekAfter triggered: checkpointRows=%d, checkpointEquivalentRows=%d, progressSource=%s, checkpointCount=%d",
            nowString(),
            targetCheckpoint.rawRows,
            targetCheckpoint.equivalentRows,
            targetCheckpoint.source,
            controller.checkpoints.size()));
  }

  private static void maybePerformScheduledSeek(
      final SubscriptionTreePullConsumer consumer,
      final PerfConfig config,
      final PerfStats stats,
      final ScheduledSeekController controller,
      final long elapsedNanoTime)
      throws Exception {
    if (!controller.enabled
        || controller.performed
        || Objects.isNull(controller.checkpoint)
        || elapsedNanoTime < config.seekTriggerNanos) {
      return;
    }

    consumer.seekAfter(config.topic, controller.checkpoint.topicProgress);

    controller.performed = true;
    stats.totalRandomSeeks++;

    System.out.println(
        String.format(
            Locale.ROOT,
            "[%s] Scheduled seekAfter triggered: checkpointRows=%d, checkpointEquivalentRows=%d, progressSource=%s, triggerSec=%.3f",
            nowString(),
            controller.checkpoint.rawRows,
            controller.checkpoint.equivalentRows,
            controller.checkpoint.source,
            config.seekTriggerSec));
  }

  private static boolean isEmptyTopicProgress(final TopicProgress topicProgress) {
    return Objects.isNull(topicProgress) || topicProgress.getRegionProgress().isEmpty();
  }

  private static void printReport(
      final String label,
      final Snapshot previous,
      final Snapshot current,
      final long elapsedNanoTime,
      final PollResult pollResult) {
    final double seconds = Math.max(1e-9, elapsedNanoTime / 1_000_000_000.0d);

    final long intervalMessages = current.totalMessages - previous.totalMessages;
    final long intervalTablets = current.totalTablets - previous.totalTablets;
    final long intervalRows = current.totalRows - previous.totalRows;
    final long intervalEquivalentRows = current.totalEquivalentRows - previous.totalEquivalentRows;
    final long intervalBytes = current.totalApproxBytes - previous.totalApproxBytes;
    final long intervalWatermarks =
        current.totalWatermarkMessages - previous.totalWatermarkMessages;
    final long intervalOutOfOrderRows = current.totalOutOfOrderRows - previous.totalOutOfOrderRows;
    final double intervalOutOfOrderRatio =
        intervalRows <= 0 ? 0d : intervalOutOfOrderRows * 100.0d / intervalRows;
    final double totalOutOfOrderRatio =
        current.totalRows <= 0 ? 0d : current.totalOutOfOrderRows * 100.0d / current.totalRows;
    final LatencySummary intervalLatency = LatencySummary.delta(previous, current);
    final LatencySummary totalLatency = LatencySummary.total(current);

    System.out.println(
        String.format(
            Locale.ROOT,
            "[%s] %-8s msgs=%d (%.1f/s), tablets=%d (%.1f/s), rows=%d (%.1f/s), eqRows=%d (%.1f/s), bytes=%s (%s/s), "
                + "watermarks=%d, oooRows=%d (%.4f%%), totalOoo=%.4f%%, maxTsBack=%d, "
                + "latRows=%d, latAvgMs=%s, latP95Ms=%s, latP99Ms=%s, latMaxMs=%s, totalLatAvgMs=%s, totalLatP95Ms=%s, totalLatP99Ms=%s, totalLatMaxMs=%s, "
                + "totalRows=%d, equivalentRows=%d, replayRows=%d, seeks=%d, totalBytes=%s, polls=%d, emptyPolls=%d, buffered=%d, watermark=%s",
            nowString(),
            label,
            intervalMessages,
            intervalMessages / seconds,
            intervalTablets,
            intervalTablets / seconds,
            intervalRows,
            intervalRows / seconds,
            intervalEquivalentRows,
            intervalEquivalentRows / seconds,
            formatBytes(intervalBytes),
            formatBytes((long) (intervalBytes / seconds)),
            intervalWatermarks,
            intervalOutOfOrderRows,
            intervalOutOfOrderRatio,
            totalOutOfOrderRatio,
            current.maxTimestampRegression,
            intervalLatency.sampleCount,
            intervalLatency.formatAverageMs(),
            intervalLatency.p95MsLabel,
            intervalLatency.p99MsLabel,
            intervalLatency.maxMsLabel,
            totalLatency.formatAverageMs(),
            totalLatency.p95MsLabel,
            totalLatency.p99MsLabel,
            totalLatency.maxMsLabel,
            current.totalRows,
            current.totalEquivalentRows,
            current.totalRows - current.totalEquivalentRows,
            current.totalRandomSeeks,
            formatBytes(current.totalApproxBytes),
            current.totalPollCalls,
            current.emptyPollCalls,
            pollResult.getBufferedCount(),
            formatWatermark(current.lastWatermark)));
  }

  private static String formatWatermark(final long watermark) {
    return watermark >= 0 ? Long.toString(watermark) : "N/A";
  }

  private static String formatBytes(final long bytes) {
    final long absBytes = Math.abs(bytes);
    if (absBytes < 1024) {
      return bytes + " B";
    }
    if (absBytes < 1024L * 1024) {
      return String.format(Locale.ROOT, "%.2f KiB", bytes / 1024.0d);
    }
    if (absBytes < 1024L * 1024 * 1024) {
      return String.format(Locale.ROOT, "%.2f MiB", bytes / 1024.0d / 1024.0d);
    }
    return String.format(Locale.ROOT, "%.2f GiB", bytes / 1024.0d / 1024.0d / 1024.0d);
  }

  private static String nowString() {
    return TIME_FORMATTER.format(Instant.now());
  }

  private static long nanosToSeconds(final long nanos) {
    return nanos / 1_000_000_000L;
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println(
        "  java ... org.apache.iotdb.ConsensusSubscriptionPerfTest [--key=value ...]");
    System.out.println();
    System.out.println("Available keys:");
    System.out.println("  host=127.0.0.1");
    System.out.println("  port=6667");
    System.out.println("  username=root");
    System.out.println("  password=root");
    System.out.println("  topic=topic_perf_<timestamp>");
    System.out.println("  group=cg_perf_<timestamp>");
    System.out.println("  consumer=consumer_perf_<timestamp>");
    System.out.println("  path=root.**");
    System.out.println("  orderMode=leader-only");
    System.out.println("  autoCreateTopic=true");
    System.out.println("  createTopicOnly=false");
    System.out.println("  autoCommit=true");
    System.out.println("  autoCommitIntervalMs=1000");
    System.out.println("  pollTimeoutMs=1000");
    System.out.println("  waitBeforePollSec=0");
    System.out.println("  reportIntervalSec=5");
    System.out.println("  durationSec=0  (0 means run until manually stopped)");
    System.out.println("  processDelayMs=0  (delay per non-watermark message, decimal allowed)");
    System.out.println("  targetPointsPerSec=0  (0 disables point-rate limiting)");
    System.out.println("  ingestWallTimeSensor=ingest_wall_time_ms");
    System.out.println("  randomSeek=false");
    System.out.println("  randomSeekMinRows=1000000");
    System.out.println("  seekCaptureRows=0  (0 disables scheduled checkpoint capture)");
    System.out.println("  seekTriggerSec=0  (0 disables scheduled seek)");
    System.out.println(
        "  consumerStopSec=0  (0 disables consumer polling pause/resume simulation)");
    System.out.println("  consumerResumeSec=0  (must be > consumerStopSec when enabled)");
    System.out.println("  consumerPauseEveryRows=0  (0 disables row-based recurring pauses)");
    System.out.println(
        "  consumerPauseDurationSec=0  (must be > 0 when consumerPauseEveryRows is enabled)");
  }

  private static final class PerfConfig {
    private final boolean help;
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String topic;
    private final String group;
    private final String consumer;
    private final String path;
    private final String orderMode;
    private final String ingestWallTimeSensor;
    private final boolean autoCreateTopic;
    private final boolean createTopicOnly;
    private final boolean autoCommit;
    private final long autoCommitIntervalMs;
    private final long pollTimeoutMs;
    private final double waitBeforePollSec;
    private final long waitBeforePollNanos;
    private final long reportIntervalSec;
    private final long durationSec;
    private final double processDelayMs;
    private final long processDelayNanos;
    private final double targetPointsPerSec;
    private final boolean randomSeek;
    private final long randomSeekMinRows;
    private final long seekCaptureRows;
    private final double seekTriggerSec;
    private final long seekTriggerNanos;
    private final double consumerStopSec;
    private final long consumerStopNanos;
    private final double consumerResumeSec;
    private final long consumerResumeNanos;
    private final long consumerPauseEveryRows;
    private final double consumerPauseDurationSec;
    private final long consumerPauseDurationNanos;

    private PerfConfig(
        final boolean help,
        final String host,
        final int port,
        final String username,
        final String password,
        final String topic,
        final String group,
        final String consumer,
        final String path,
        final String orderMode,
        final String ingestWallTimeSensor,
        final boolean autoCreateTopic,
        final boolean createTopicOnly,
        final boolean autoCommit,
        final long autoCommitIntervalMs,
        final long pollTimeoutMs,
        final double waitBeforePollSec,
        final long waitBeforePollNanos,
        final long reportIntervalSec,
        final long durationSec,
        final double processDelayMs,
        final long processDelayNanos,
        final double targetPointsPerSec,
        final boolean randomSeek,
        final long randomSeekMinRows,
        final long seekCaptureRows,
        final double seekTriggerSec,
        final long seekTriggerNanos,
        final double consumerStopSec,
        final long consumerStopNanos,
        final double consumerResumeSec,
        final long consumerResumeNanos,
        final long consumerPauseEveryRows,
        final double consumerPauseDurationSec,
        final long consumerPauseDurationNanos) {
      this.help = help;
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.topic = topic;
      this.group = group;
      this.consumer = consumer;
      this.path = path;
      this.orderMode = orderMode;
      this.ingestWallTimeSensor = ingestWallTimeSensor;
      this.autoCreateTopic = autoCreateTopic;
      this.createTopicOnly = createTopicOnly;
      this.autoCommit = autoCommit;
      this.autoCommitIntervalMs = autoCommitIntervalMs;
      this.pollTimeoutMs = pollTimeoutMs;
      this.waitBeforePollSec = waitBeforePollSec;
      this.waitBeforePollNanos = waitBeforePollNanos;
      this.reportIntervalSec = reportIntervalSec;
      this.durationSec = durationSec;
      this.processDelayMs = processDelayMs;
      this.processDelayNanos = processDelayNanos;
      this.targetPointsPerSec = targetPointsPerSec;
      this.randomSeek = randomSeek;
      this.randomSeekMinRows = randomSeekMinRows;
      this.seekCaptureRows = seekCaptureRows;
      this.seekTriggerSec = seekTriggerSec;
      this.seekTriggerNanos = seekTriggerNanos;
      this.consumerStopSec = consumerStopSec;
      this.consumerStopNanos = consumerStopNanos;
      this.consumerResumeSec = consumerResumeSec;
      this.consumerResumeNanos = consumerResumeNanos;
      this.consumerPauseEveryRows = consumerPauseEveryRows;
      this.consumerPauseDurationSec = consumerPauseDurationSec;
      this.consumerPauseDurationNanos = consumerPauseDurationNanos;
    }

    private static PerfConfig parse(final String[] args) {
      final long suffix = System.currentTimeMillis();
      String host = "127.0.0.1";
      int port = 6667;
      String username = "root";
      String password = "root";
      String topic = "topic_perf_" + suffix;
      String group = "cg_perf_" + suffix;
      String consumer = "consumer_perf_" + suffix;
      String path = "root.**";
      String orderMode = TopicConstant.ORDER_MODE_DEFAULT_VALUE;
      orderMode = TopicConstant.ORDER_MODE_PER_WRITER_VALUE;
      String ingestWallTimeSensor = "ingest_wall_time_ms";
      boolean autoCreateTopic = true;
      boolean createTopicOnly = false;
      boolean autoCommit = true;
      long autoCommitIntervalMs = 1000L;
      long pollTimeoutMs = 1000L;
      double waitBeforePollSec = 0d;
      long reportIntervalSec = 1L;
      long durationSec = 0L;
      double processDelayMs = 0d;
      double targetPointsPerSec = 10_000_000d;
      boolean randomSeek = false;
      long randomSeekMinRows = 2_000_000L;
      long seekCaptureRows = 0L;
      double seekTriggerSec = 0d;
      double consumerStopSec = 0d;
      double consumerResumeSec = 0d;
      long consumerPauseEveryRows = 0L;
      double consumerPauseDurationSec = 0d;
      boolean help = false;

      for (final String arg : args) {
        if ("--help".equals(arg) || "-h".equals(arg)) {
          help = true;
          continue;
        }

        final String normalized = arg.startsWith("--") ? arg.substring(2) : arg;
        final int separator = normalized.indexOf('=');
        if (separator <= 0) {
          throw new IllegalArgumentException(
              "Invalid argument: " + arg + ". Expected format --key=value");
        }

        final String key = normalized.substring(0, separator);
        final String value = normalized.substring(separator + 1);

        switch (key) {
          case "host":
            host = value;
            break;
          case "port":
            port = Integer.parseInt(value);
            break;
          case "username":
            username = value;
            break;
          case "password":
            password = value;
            break;
          case "topic":
            topic = value;
            break;
          case "group":
            group = value;
            break;
          case "consumer":
            consumer = value;
            break;
          case "path":
            path = value;
            break;
          case "orderMode":
          case "order-mode":
            orderMode = TopicConfig.normalizeOrderMode(value);
            break;
          case "ingestWallTimeSensor":
          case "ingest-wall-time-sensor":
            ingestWallTimeSensor = value;
            break;
          case "autoCreateTopic":
            autoCreateTopic = Boolean.parseBoolean(value);
            break;
          case "createTopicOnly":
            createTopicOnly = Boolean.parseBoolean(value);
            break;
          case "autoCommit":
            autoCommit = Boolean.parseBoolean(value);
            break;
          case "autoCommitIntervalMs":
            autoCommitIntervalMs = Long.parseLong(value);
            break;
          case "pollTimeoutMs":
            pollTimeoutMs = Long.parseLong(value);
            break;
          case "waitBeforePollSec":
            waitBeforePollSec = Double.parseDouble(value);
            break;
          case "reportIntervalSec":
            reportIntervalSec = Long.parseLong(value);
            break;
          case "durationSec":
            durationSec = Long.parseLong(value);
            break;
          case "processDelayMs":
            processDelayMs = Double.parseDouble(value);
            break;
          case "targetPointsPerSec":
          case "target-points-per-sec":
            targetPointsPerSec = Double.parseDouble(value);
            break;
          case "randomSeek":
            randomSeek = Boolean.parseBoolean(value);
            break;
          case "randomSeekMinRows":
            randomSeekMinRows = Long.parseLong(value);
            break;
          case "seekCaptureRows":
            seekCaptureRows = Long.parseLong(value);
            break;
          case "seekTriggerSec":
            seekTriggerSec = Double.parseDouble(value);
            break;
          case "consumerStopSec":
          case "consumer-stop-sec":
            consumerStopSec = Double.parseDouble(value);
            break;
          case "consumerResumeSec":
          case "consumer-resume-sec":
            consumerResumeSec = Double.parseDouble(value);
            break;
          case "consumerPauseEveryRows":
          case "consumer-pause-every-rows":
            consumerPauseEveryRows = Long.parseLong(value);
            break;
          case "consumerPauseDurationSec":
          case "consumer-pause-duration-sec":
            consumerPauseDurationSec = Double.parseDouble(value);
            break;
          default:
            throw new IllegalArgumentException("Unknown argument key: " + key);
        }
      }

      if (!TopicConfig.isValidOrderMode(orderMode)) {
        throw new IllegalArgumentException("Unsupported orderMode: " + orderMode);
      }
      if (processDelayMs < 0) {
        throw new IllegalArgumentException("processDelayMs must be >= 0");
      }
      if (targetPointsPerSec < 0) {
        throw new IllegalArgumentException("targetPointsPerSec must be >= 0");
      }
      if (waitBeforePollSec < 0) {
        throw new IllegalArgumentException("waitBeforePollSec must be >= 0");
      }
      if (randomSeekMinRows < 0) {
        throw new IllegalArgumentException("randomSeekMinRows must be >= 0");
      }
      if (seekCaptureRows < 0) {
        throw new IllegalArgumentException("seekCaptureRows must be >= 0");
      }
      if (seekTriggerSec < 0) {
        throw new IllegalArgumentException("seekTriggerSec must be >= 0");
      }
      if (consumerStopSec < 0) {
        throw new IllegalArgumentException("consumerStopSec must be >= 0");
      }
      if (consumerResumeSec < 0) {
        throw new IllegalArgumentException("consumerResumeSec must be >= 0");
      }
      if (consumerPauseEveryRows < 0) {
        throw new IllegalArgumentException("consumerPauseEveryRows must be >= 0");
      }
      if (consumerPauseDurationSec < 0) {
        throw new IllegalArgumentException("consumerPauseDurationSec must be >= 0");
      }
      if ((seekCaptureRows > 0) != (seekTriggerSec > 0)) {
        throw new IllegalArgumentException(
            "seekCaptureRows and seekTriggerSec must both be set to positive values to enable scheduled seek");
      }
      if ((consumerStopSec > 0) != (consumerResumeSec > 0)) {
        throw new IllegalArgumentException(
            "consumerStopSec and consumerResumeSec must both be set to positive values to enable consumer polling pause/resume simulation");
      }
      if (consumerResumeSec > 0 && consumerResumeSec <= consumerStopSec) {
        throw new IllegalArgumentException(
            "consumerResumeSec must be greater than consumerStopSec");
      }
      if ((consumerPauseEveryRows > 0) != (consumerPauseDurationSec > 0)) {
        throw new IllegalArgumentException(
            "consumerPauseEveryRows and consumerPauseDurationSec must both be set to positive values to enable row-based recurring pauses");
      }
      if (consumerPauseEveryRows > 0 && consumerStopSec > 0) {
        throw new IllegalArgumentException(
            "consumerPauseEveryRows/consumerPauseDurationSec cannot be combined with consumerStopSec/consumerResumeSec");
      }

      final long waitBeforePollNanos = Math.round(waitBeforePollSec * 1_000_000_000.0d);
      final long processDelayNanos = Math.round(processDelayMs * 1_000_000.0d);
      final long seekTriggerNanos = Math.round(seekTriggerSec * 1_000_000_000.0d);
      final long consumerStopNanos = Math.round(consumerStopSec * 1_000_000_000.0d);
      final long consumerResumeNanos = Math.round(consumerResumeSec * 1_000_000_000.0d);
      final long consumerPauseDurationNanos =
          Math.round(consumerPauseDurationSec * 1_000_000_000.0d);

      return new PerfConfig(
          help,
          host,
          port,
          username,
          password,
          topic,
          group,
          consumer,
          path,
          orderMode,
          ingestWallTimeSensor,
          autoCreateTopic,
          createTopicOnly,
          autoCommit,
          autoCommitIntervalMs,
          pollTimeoutMs,
          waitBeforePollSec,
          waitBeforePollNanos,
          reportIntervalSec,
          durationSec,
          processDelayMs,
          processDelayNanos,
          targetPointsPerSec,
          randomSeek,
          randomSeekMinRows,
          seekCaptureRows,
          seekTriggerSec,
          seekTriggerNanos,
          consumerStopSec,
          consumerStopNanos,
          consumerResumeSec,
          consumerResumeNanos,
          consumerPauseEveryRows,
          consumerPauseDurationSec,
          consumerPauseDurationNanos);
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "Config{host=%s, port=%d, username=%s, topic=%s, group=%s, consumer=%s, path=%s, "
              + "orderMode=%s, ingestWallTimeSensor=%s, autoCreateTopic=%s, createTopicOnly=%s, autoCommit=%s, autoCommitIntervalMs=%d, pollTimeoutMs=%d, "
              + "waitBeforePollSec=%.3f, "
              + "reportIntervalSec=%d, durationSec=%d, processDelayMs=%.3f, targetPointsPerSec=%.3f, randomSeek=%s, randomSeekMinRows=%d, seekCaptureRows=%d, seekTriggerSec=%.3f, consumerStopSec=%.3f, consumerResumeSec=%.3f, consumerPauseEveryRows=%d, consumerPauseDurationSec=%.3f}",
          host,
          port,
          username,
          topic,
          group,
          consumer,
          path,
          orderMode,
          ingestWallTimeSensor,
          autoCreateTopic,
          createTopicOnly,
          autoCommit,
          autoCommitIntervalMs,
          pollTimeoutMs,
          waitBeforePollSec,
          reportIntervalSec,
          durationSec,
          processDelayMs,
          targetPointsPerSec,
          randomSeek,
          randomSeekMinRows,
          seekCaptureRows,
          seekTriggerSec,
          consumerStopSec,
          consumerResumeSec,
          consumerPauseEveryRows,
          consumerPauseDurationSec);
    }

    private boolean enableEquivalentRowTracking() {
      return randomSeek || (seekCaptureRows > 0 && seekTriggerSec > 0);
    }
  }

  private static final class ProcessingRateLimiter {
    private final double targetPointsPerSec;
    private long throttlingStartNanoTime = -1L;
    private long totalProcessedPoints = 0L;

    private ProcessingRateLimiter(final double targetPointsPerSec) {
      this.targetPointsPerSec = targetPointsPerSec;
    }

    private boolean isEnabled() {
      return targetPointsPerSec > 0d;
    }

    private void acquire(final long processedPoints) {
      if (!isEnabled() || processedPoints <= 0) {
        return;
      }

      final long nowNanoTime = System.nanoTime();
      if (throttlingStartNanoTime < 0) {
        throttlingStartNanoTime = nowNanoTime;
      }

      totalProcessedPoints += processedPoints;
      final long targetElapsedNanos =
          (long) Math.ceil((totalProcessedPoints * 1_000_000_000.0d) / targetPointsPerSec);
      final long actualElapsedNanos = nowNanoTime - throttlingStartNanoTime;
      final long remainingNanos = targetElapsedNanos - actualElapsedNanos;
      if (remainingNanos > 0) {
        LockSupport.parkNanos(remainingNanos);
      }
    }

    private void pauseForDowntime(final long pausedNanos) {
      if (!isEnabled() || throttlingStartNanoTime < 0 || pausedNanos <= 0) {
        return;
      }
      throttlingStartNanoTime += pausedNanos;
    }
  }

  private static final class PerfStats {
    private long totalPollCalls;
    private long emptyPollCalls;
    private long totalMessages;
    private long totalWatermarkMessages;
    private long totalTsFileMessages;
    private long totalTablets;
    private long totalRows;
    private long totalEquivalentRows;
    private long totalApproxBytes;
    private long totalOutOfOrderRows;
    private long maxTimestampRegression;
    private long totalLatencySamples;
    private long totalLatencySumMs;
    private final long[] latencyHistogramBuckets = new long[LatencyHistogram.BUCKET_COUNT];
    private int lastBufferedCount;
    private long lastWatermark = -1L;
    private final Map<String, Long> lastSeenTimestampByDevice = new HashMap<>();
    private final EquivalentRowTracker equivalentRowTracker;
    private long totalRandomSeeks;
    private long totalRandomSeekCheckpoints;
    private long lastRandomSeekSourceRows = -1L;
    private long lastRandomSeekEquivalentRows = -1L;
    private long lastRandomSeekObservedRows = -1L;

    private PerfStats(final boolean enableEquivalentRowTracking) {
      this.equivalentRowTracker = enableEquivalentRowTracking ? new EquivalentRowTracker() : null;
    }

    private void recordLatency(final long latencyMs) {
      totalLatencySamples++;
      totalLatencySumMs += latencyMs;
      latencyHistogramBuckets[LatencyHistogram.bucketIndex(latencyMs)]++;
    }
  }

  private static final class Snapshot {
    private final long totalPollCalls;
    private final long emptyPollCalls;
    private final long totalMessages;
    private final long totalWatermarkMessages;
    private final long totalTablets;
    private final long totalRows;
    private final long totalEquivalentRows;
    private final long totalApproxBytes;
    private final long totalOutOfOrderRows;
    private final long maxTimestampRegression;
    private final long totalLatencySamples;
    private final long totalLatencySumMs;
    private final long[] latencyHistogramBuckets;
    private final long lastWatermark;
    private final long totalRandomSeeks;

    private Snapshot(
        final long totalPollCalls,
        final long emptyPollCalls,
        final long totalMessages,
        final long totalWatermarkMessages,
        final long totalTablets,
        final long totalRows,
        final long totalEquivalentRows,
        final long totalApproxBytes,
        final long totalOutOfOrderRows,
        final long maxTimestampRegression,
        final long totalLatencySamples,
        final long totalLatencySumMs,
        final long[] latencyHistogramBuckets,
        final long lastWatermark,
        final long totalRandomSeeks) {
      this.totalPollCalls = totalPollCalls;
      this.emptyPollCalls = emptyPollCalls;
      this.totalMessages = totalMessages;
      this.totalWatermarkMessages = totalWatermarkMessages;
      this.totalTablets = totalTablets;
      this.totalRows = totalRows;
      this.totalEquivalentRows = totalEquivalentRows;
      this.totalApproxBytes = totalApproxBytes;
      this.totalOutOfOrderRows = totalOutOfOrderRows;
      this.maxTimestampRegression = maxTimestampRegression;
      this.totalLatencySamples = totalLatencySamples;
      this.totalLatencySumMs = totalLatencySumMs;
      this.latencyHistogramBuckets = latencyHistogramBuckets;
      this.lastWatermark = lastWatermark;
      this.totalRandomSeeks = totalRandomSeeks;
    }

    private static Snapshot capture(final PerfStats stats) {
      Objects.requireNonNull(stats, "stats");
      return new Snapshot(
          stats.totalPollCalls,
          stats.emptyPollCalls,
          stats.totalMessages,
          stats.totalWatermarkMessages,
          stats.totalTablets,
          stats.totalRows,
          stats.totalEquivalentRows,
          stats.totalApproxBytes,
          stats.totalOutOfOrderRows,
          stats.maxTimestampRegression,
          stats.totalLatencySamples,
          stats.totalLatencySumMs,
          Arrays.copyOf(stats.latencyHistogramBuckets, stats.latencyHistogramBuckets.length),
          stats.lastWatermark,
          stats.totalRandomSeeks);
    }

    private static Snapshot zero() {
      return new Snapshot(
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new long[LatencyHistogram.BUCKET_COUNT], -1L, 0);
    }
  }

  private static final class RandomSeekController {
    private final boolean enabled;
    private final Random random = new Random();
    private final List<SeekCheckpoint> checkpoints = new ArrayList<>();
    private boolean performed;
    private long lastCapturedRows = Long.MIN_VALUE;
    private TopicProgress lastCapturedProgress;

    private RandomSeekController(final boolean enabled) {
      this.enabled = enabled;
    }
  }

  private static final class ScheduledSeekController {
    private final boolean enabled;
    private boolean performed;
    private SeekCheckpoint checkpoint;

    private ScheduledSeekController(final boolean enabled) {
      this.enabled = enabled;
    }
  }

  private static final class ConsumerRestartController {
    private final boolean enabled;
    private boolean stopPerformed;
    private boolean resumePerformed;
    private long stoppedNanoTime = -1L;

    private ConsumerRestartController(final boolean enabled) {
      this.enabled = enabled;
    }
  }

  private static final class ConsumerPauseController {
    private final boolean enabled;
    private long nextPauseRows;
    private boolean paused;
    private long stoppedNanoTime = -1L;
    private long pausePerformedCount;

    private ConsumerPauseController(final long pauseEveryRows) {
      this.enabled = pauseEveryRows > 0;
      this.nextPauseRows = pauseEveryRows;
    }
  }

  private static final class SeekCheckpoint {
    private final long rawRows;
    private final long equivalentRows;
    private final String source;
    private final TopicProgress topicProgress;

    private SeekCheckpoint(
        final long rawRows,
        final long equivalentRows,
        final String source,
        final TopicProgress topicProgress) {
      this.rawRows = rawRows;
      this.equivalentRows = equivalentRows;
      this.source = source;
      this.topicProgress = topicProgress;
    }
  }

  private static final class EquivalentRowTracker {
    private final Map<String, NavigableMap<Long, Long>> intervalsByDevice = new HashMap<>();

    private boolean record(final String deviceId, final long timestamp) {
      final NavigableMap<Long, Long> intervals =
          intervalsByDevice.computeIfAbsent(deviceId, ignored -> new TreeMap<>());
      final Map.Entry<Long, Long> floor = intervals.floorEntry(timestamp);
      if (Objects.nonNull(floor) && floor.getValue() >= timestamp) {
        return false;
      }

      long start = timestamp;
      long end = timestamp;

      if (Objects.nonNull(floor) && floor.getValue() + 1 == timestamp) {
        start = floor.getKey();
        intervals.remove(floor.getKey());
      }

      final Map.Entry<Long, Long> ceiling = intervals.ceilingEntry(timestamp);
      if (Objects.nonNull(ceiling) && ceiling.getKey() - 1 == timestamp) {
        end = ceiling.getValue();
        intervals.remove(ceiling.getKey());
      }

      intervals.put(start, end);
      return true;
    }
  }

  private static final class LatencyHistogram {
    private static final int MAX_TRACKED_LATENCY_MS = 60_000;
    private static final int BUCKET_COUNT = MAX_TRACKED_LATENCY_MS + 2;

    private static int bucketIndex(final long latencyMs) {
      if (latencyMs <= 0) {
        return 0;
      }
      if (latencyMs > MAX_TRACKED_LATENCY_MS) {
        return MAX_TRACKED_LATENCY_MS + 1;
      }
      return (int) latencyMs;
    }

    private static String bucketLabel(final int bucketIndex) {
      if (bucketIndex > MAX_TRACKED_LATENCY_MS) {
        return ">" + MAX_TRACKED_LATENCY_MS;
      }
      return Integer.toString(bucketIndex);
    }
  }

  private static final class LatencySummary {
    private final long sampleCount;
    private final long sumMs;
    private final String p95MsLabel;
    private final String p99MsLabel;
    private final String maxMsLabel;

    private LatencySummary(
        final long sampleCount,
        final long sumMs,
        final String p95MsLabel,
        final String p99MsLabel,
        final String maxMsLabel) {
      this.sampleCount = sampleCount;
      this.sumMs = sumMs;
      this.p95MsLabel = p95MsLabel;
      this.p99MsLabel = p99MsLabel;
      this.maxMsLabel = maxMsLabel;
    }

    private static LatencySummary delta(final Snapshot previous, final Snapshot current) {
      final long sampleCount = current.totalLatencySamples - previous.totalLatencySamples;
      final long sumMs = current.totalLatencySumMs - previous.totalLatencySumMs;
      if (sampleCount <= 0) {
        return empty();
      }
      return summarize(
          sampleCount, sumMs, current.latencyHistogramBuckets, previous.latencyHistogramBuckets);
    }

    private static LatencySummary total(final Snapshot current) {
      if (current.totalLatencySamples <= 0) {
        return empty();
      }
      return summarize(
          current.totalLatencySamples,
          current.totalLatencySumMs,
          current.latencyHistogramBuckets,
          null);
    }

    private static LatencySummary summarize(
        final long sampleCount,
        final long sumMs,
        final long[] currentBuckets,
        final long[] previousBuckets) {
      final long p95Threshold = Math.max(1L, (long) Math.ceil(sampleCount * 0.95d));
      final long p99Threshold = Math.max(1L, (long) Math.ceil(sampleCount * 0.99d));
      long cumulative = 0L;
      String p95 = "N/A";
      String p99 = "N/A";
      String max = "N/A";

      for (int bucketIndex = 0; bucketIndex < currentBuckets.length; bucketIndex++) {
        final long bucketCount =
            currentBuckets[bucketIndex]
                - (previousBuckets == null ? 0L : previousBuckets[bucketIndex]);
        if (bucketCount <= 0) {
          continue;
        }

        cumulative += bucketCount;
        if ("N/A".equals(p95) && cumulative >= p95Threshold) {
          p95 = LatencyHistogram.bucketLabel(bucketIndex);
        }
        if ("N/A".equals(p99) && cumulative >= p99Threshold) {
          p99 = LatencyHistogram.bucketLabel(bucketIndex);
        }
      }

      for (int bucketIndex = currentBuckets.length - 1; bucketIndex >= 0; bucketIndex--) {
        final long bucketCount =
            currentBuckets[bucketIndex]
                - (previousBuckets == null ? 0L : previousBuckets[bucketIndex]);
        if (bucketCount > 0) {
          max = LatencyHistogram.bucketLabel(bucketIndex);
          break;
        }
      }

      return new LatencySummary(sampleCount, sumMs, p95, p99, max);
    }

    private static LatencySummary empty() {
      return new LatencySummary(0L, 0L, "N/A", "N/A", "N/A");
    }

    private String formatAverageMs() {
      if (sampleCount <= 0) {
        return "N/A";
      }
      return String.format(Locale.ROOT, "%.2f", sumMs / (double) sampleCount);
    }
  }
}
