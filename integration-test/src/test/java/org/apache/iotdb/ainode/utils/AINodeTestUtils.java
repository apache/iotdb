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

package org.apache.iotdb.ainode.utils;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.Constant;

import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.it.utils.TestUtils.assertQueryFails;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AINodeTestUtils {

  public static final Map<String, FakeModelInfo> BUILTIN_LTSM_MAP =
      Stream.of(
              new AbstractMap.SimpleEntry<>(
                  "timer_xl", new FakeModelInfo("timer_xl", "timer", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "sundial", new FakeModelInfo("sundial", "sundial", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "chronos2", new FakeModelInfo("chronos2", "t5", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "moirai2", new FakeModelInfo("moirai2", "moirai", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "toto", new FakeModelInfo("toto", "toto", "builtin", "active")),
              new AbstractMap.SimpleEntry<>(
                  "moment", new FakeModelInfo("moment", "moment", "builtin", "active")))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  /**
   * Built-in models whose weights the user provides: AINode never downloads them, and they are
   * inactive until their files are installed, which they are not in these tests.
   */
  public static final Map<String, FakeModelInfo> USER_PROVIDED_BUILTIN_MODEL_MAP =
      Stream.of(
              new AbstractMap.SimpleEntry<>(
                  "timestral", new FakeModelInfo("timestral", "timestral", "builtin", "inactive")))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public static final Map<String, FakeModelInfo> BUILTIN_MODEL_MAP;

  static {
    Map<String, FakeModelInfo> tmp =
        Stream.of(
                new AbstractMap.SimpleEntry<>(
                    "arima", new FakeModelInfo("arima", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "holtwinters", new FakeModelInfo("holtwinters", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "exponential_smoothing",
                    new FakeModelInfo("exponential_smoothing", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "naive_forecaster",
                    new FakeModelInfo("naive_forecaster", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "stl_forecaster",
                    new FakeModelInfo("stl_forecaster", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "gaussian_hmm",
                    new FakeModelInfo("gaussian_hmm", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "gmm_hmm", new FakeModelInfo("gmm_hmm", "sktime", "builtin", "active")),
                new AbstractMap.SimpleEntry<>(
                    "stray", new FakeModelInfo("stray", "sktime", "builtin", "active")))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    tmp.putAll(BUILTIN_LTSM_MAP);
    BUILTIN_MODEL_MAP = Collections.unmodifiableMap(tmp);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeTestUtils.class);

  private static final long CONCURRENT_INFERENCE_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final int CONCURRENT_INFERENCE_QUERY_TIMEOUT_SECONDS = 60;
  // Give worker connections a finite socket timeout above the server query timeout so a hung
  // response cannot block reads indefinitely; keep drain > 4x network timeout to cover the initial
  // RPC plus IoTDBConnection.reconnect()'s 3 retry attempts.
  private static final int CONCURRENT_INFERENCE_NETWORK_TIMEOUT_MILLIS =
      (int) TimeUnit.SECONDS.toMillis(CONCURRENT_INFERENCE_QUERY_TIMEOUT_SECONDS + 10L);
  private static final long CONCURRENT_INFERENCE_WORKER_DRAIN_TIMEOUT_MILLIS =
      TimeUnit.SECONDS.toMillis(300);
  private static final long MODEL_STATUS_POLL_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(3);
  private static final long MODEL_LOAD_SETTLE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(600);
  private static final long MODEL_UNLOAD_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(150);

  @FunctionalInterface
  interface ConnectionSupplier {
    Connection get() throws SQLException;
  }

  public static final class SurvivingInferenceWorkersException extends IllegalStateException {

    private final List<String> survivingWorkers;

    SurvivingInferenceWorkersException(List<String> survivingWorkers) {
      super("Inference workers still alive after cancellation drain timeout: " + survivingWorkers);
      this.survivingWorkers = Collections.unmodifiableList(new ArrayList<>(survivingWorkers));
    }

    public List<String> getSurvivingWorkers() {
      return survivingWorkers;
    }
  }

  public static boolean hasSurvivingInferenceWorkers(Throwable throwable) {
    if (throwable == null) {
      return false;
    }
    if (throwable instanceof SurvivingInferenceWorkersException) {
      return true;
    }
    for (Throwable suppressed : throwable.getSuppressed()) {
      if (hasSurvivingInferenceWorkers(suppressed)) {
        return true;
      }
    }
    return hasSurvivingInferenceWorkers(throwable.getCause());
  }

  public static void checkHeader(ResultSetMetaData resultSetMetaData, String title)
      throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  public static void errorTest(Statement statement, String sql, String errorMessage) {
    assertEquals(errorMessage, assertQueryFails(statement, sql).getMessage());
  }

  public static void concurrentInference(
      String sqlDialect, String sql, int threadCnt, int loop, int expectedOutputLength)
      throws InterruptedException {
    // Use the Version + networkTimeoutMs overload so profiles with
    // ReadAndVerifyWithMultiNode=false (such as AIClusterIT) return direct IoTDBConnections
    // (avoiding the 5-thread cap in RequestThreadPool) with bounded socket reads.
    concurrentInference(
        () ->
            EnvFactory.getEnv()
                .getConnection(
                    Constant.Version.V_1_0,
                    sqlDialect,
                    CONCURRENT_INFERENCE_NETWORK_TIMEOUT_MILLIS),
        sql,
        threadCnt,
        loop,
        expectedOutputLength,
        CONCURRENT_INFERENCE_TIMEOUT_MILLIS,
        CONCURRENT_INFERENCE_QUERY_TIMEOUT_SECONDS,
        CONCURRENT_INFERENCE_WORKER_DRAIN_TIMEOUT_MILLIS);
  }

  static void concurrentInference(
      ConnectionSupplier connectionSupplier,
      String sql,
      int threadCnt,
      int loop,
      int expectedOutputLength,
      long overallTimeoutMillis,
      int perQueryTimeoutSeconds,
      long workerDrainTimeoutMillis)
      throws InterruptedException {
    AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicInteger completedRequests = new AtomicInteger(0);
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    int boundedPerQueryTimeoutSeconds = Math.max(1, perQueryTimeoutSeconds);
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(overallTimeoutMillis);
    Thread[] threads = new Thread[threadCnt];
    for (int i = 0; i < threadCnt; i++) {
      threads[i] =
          new Thread(
              () -> {
                if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                  return;
                }
                // JDBC Statements cannot be shared concurrently because executing a new query
                // closes any previously open ResultSet on that Statement.
                try (Connection connection = connectionSupplier.get();
                    Statement statement = connection.createStatement()) {
                  for (int j = 0; j < loop; j++) {
                    if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                      return;
                    }
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                      if (!cancelled.get()) {
                        firstFailure.compareAndSet(
                            null,
                            new AssertionError(
                                "Concurrent inference deadline exceeded before completing all loops for SQL: "
                                    + sql));
                      }
                      return;
                    }
                    long remainingSecondsCeil = (remainingNanos - 1L) / 1_000_000_000L + 1L;
                    int queryTimeoutSeconds =
                        (int) Math.min(boundedPerQueryTimeoutSeconds, remainingSecondsCeil);
                    statement.setQueryTimeout(queryTimeoutSeconds);
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                      int outputCnt = 0;
                      while (!cancelled.get()
                          && !Thread.currentThread().isInterrupted()
                          && resultSet.next()) {
                        outputCnt++;
                      }
                      if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                        return;
                      }
                      if (expectedOutputLength != outputCnt) {
                        AssertionError error =
                            new AssertionError(
                                "Output count mismatch for SQL: "
                                    + sql
                                    + ". Expected: "
                                    + expectedOutputLength
                                    + ", but got: "
                                    + outputCnt);
                        firstFailure.compareAndSet(null, error);
                        LOGGER.error("Concurrent inference output mismatch", error);
                        return;
                      }
                      completedRequests.incrementAndGet();
                    }
                  }
                } catch (Throwable t) {
                  if (!cancelled.get()) {
                    firstFailure.compareAndSet(null, t);
                    LOGGER.error("Concurrent inference failed for SQL: {}", sql, t);
                  } else {
                    LOGGER.debug(
                        "Concurrent inference worker stopped after cancellation for SQL: {}",
                        sql,
                        t);
                  }
                }
              },
              "ainode-concurrent-inference-" + i);
      threads[i].start();
    }

    List<String> timedOutThreads = new ArrayList<>();
    try {
      for (Thread thread : threads) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos > 0L) {
          thread.join(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        }
        if (thread.isAlive()) {
          timedOutThreads.add(thread.getName());
        }
      }
    } catch (InterruptedException e) {
      cancelled.set(true);
      interruptAll(threads);
      List<String> survivingThreads =
          drainWorkersUninterruptibly(threads, workerDrainTimeoutMillis);
      Throwable failure = firstFailure.get();
      if (failure != null) {
        e.addSuppressed(failure);
      }
      if (!survivingThreads.isEmpty()) {
        e.addSuppressed(new SurvivingInferenceWorkersException(survivingThreads));
      }
      throw e;
    }

    if (!timedOutThreads.isEmpty()) {
      cancelled.set(true);
      interruptAll(threads);
      List<String> survivingThreads =
          drainWorkersUninterruptibly(threads, workerDrainTimeoutMillis);
      StringBuilder message =
          new StringBuilder("Thread timeout after ")
              .append(overallTimeoutMillis)
              .append(" ms: ")
              .append(timedOutThreads);
      if (!survivingThreads.isEmpty()) {
        message.append("; still alive after drain timeout: ").append(survivingThreads);
      }
      Throwable failure = firstFailure.get();
      AssertionError timeoutError;
      if (failure != null) {
        message.append("; first worker failure: ").append(failure.getMessage());
        timeoutError = new AssertionError(message.toString(), failure);
      } else {
        timeoutError = new AssertionError(message.toString());
      }
      if (!survivingThreads.isEmpty()) {
        timeoutError.addSuppressed(new SurvivingInferenceWorkersException(survivingThreads));
      }
      throw timeoutError;
    }

    Throwable failure = firstFailure.get();
    if (failure != null) {
      throw new AssertionError("Concurrent inference failed: " + failure.getMessage(), failure);
    }
    assertEquals(
        "Completed concurrent inference request count mismatch for SQL: " + sql,
        threadCnt * loop,
        completedRequests.get());
  }

  private static void interruptAll(Thread[] threads) {
    for (Thread thread : threads) {
      if (thread != null) {
        thread.interrupt();
      }
    }
  }

  private static List<String> drainWorkersUninterruptibly(
      Thread[] threads, long drainTimeoutMillis) {
    long drainDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(drainTimeoutMillis);
    boolean interrupted = false;
    try {
      for (Thread thread : threads) {
        if (thread == null) {
          continue;
        }
        while (thread.isAlive()) {
          long remainingNanos = drainDeadlineNanos - System.nanoTime();
          if (remainingNanos <= 0L) {
            break;
          }
          try {
            thread.join(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    List<String> survivingThreads = new ArrayList<>();
    for (Thread thread : threads) {
      if (thread != null && thread.isAlive()) {
        survivingThreads.add(thread.getName());
      }
    }
    return survivingThreads;
  }

  public static void checkModelOnSpecifiedDevice(Statement statement, String modelId, String device)
      throws SQLException, InterruptedException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    LOGGER.info("Checking model: {} on target devices: {}", modelId, targetDevices);
    for (int retry = 0; retry < 200; retry++) {
      Set<String> foundDevices = new HashSet<>();
      Map<String, Integer> deviceCounts =
          queryModelDeviceCounts(statement, modelId, device, targetDevices);
      for (Map.Entry<String, Integer> entry : deviceCounts.entrySet()) {
        if (entry.getValue() > 0) {
          foundDevices.add(entry.getKey());
          LOGGER.info("Model {} is loaded to device {}", modelId, entry.getKey());
        }
      }
      if (foundDevices.containsAll(targetDevices)) {
        LOGGER.info("Model {} is loaded to devices {}, start testing", modelId, targetDevices);
        return;
      }
      TimeUnit.SECONDS.sleep(3);
    }
    fail("Model " + modelId + " is not loaded on device " + device);
  }

  public static void checkModelNotOnSpecifiedDevice(
      Statement statement, String modelId, String device)
      throws SQLException, InterruptedException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    LOGGER.info("Checking model: {} not on target devices: {}", modelId, targetDevices);
    for (int retry = 0; retry < 50; retry++) {
      Map<String, Integer> deviceCounts =
          queryModelDeviceCounts(statement, modelId, device, targetDevices);
      if (deviceCounts.isEmpty()) {
        LOGGER.info("Model {} is unloaded from devices {}.", modelId, targetDevices);
        return;
      }
      LOGGER.info("Model {} is still present on devices {}", modelId, deviceCounts);
      TimeUnit.SECONDS.sleep(3);
    }
    fail("Model " + modelId + " is still loaded on device " + device);
  }

  public static void unloadSubmittedModel(
      Statement statement,
      String modelId,
      String device,
      boolean loadCompleted,
      long loadSubmittedNanos)
      throws SQLException {
    unloadSubmittedModel(
        statement,
        modelId,
        device,
        loadCompleted,
        loadSubmittedNanos,
        MODEL_STATUS_POLL_INTERVAL_MILLIS,
        MODEL_LOAD_SETTLE_TIMEOUT_MILLIS,
        MODEL_UNLOAD_TIMEOUT_MILLIS);
  }

  static void unloadSubmittedModel(
      Statement statement,
      String modelId,
      String device,
      boolean loadCompleted,
      long loadSubmittedNanos,
      long pollIntervalMillis,
      long loadSettleTimeoutMillis,
      long unloadTimeoutMillis)
      throws SQLException {
    Set<String> targetDevices = ImmutableSet.copyOf(device.split(","));
    AtomicBoolean interrupted = new AtomicBoolean(Thread.interrupted());
    try {
      if (!loadCompleted) {
        // LOAD MODEL returns as soon as the task is queued in AINode's PoolController. If we
        // issue UNLOAD MODEL before any device registers a pool, AINode skips queuing an unload
        // and the later load would leave the model resident for subsequent tests. Wait for the
        // submitted load to complete or settle before unloading; if it remains unresolved with no
        // running pool at the deadline, fail cleanup so the caller resets the shared cluster.
        waitForSubmittedModelLoadToSettle(
            statement,
            modelId,
            device,
            targetDevices,
            loadSubmittedNanos,
            pollIntervalMillis,
            loadSettleTimeoutMillis,
            interrupted);
      }
      statement.execute(String.format("UNLOAD MODEL %s FROM DEVICES '%s'", modelId, device));
      waitForModelRemoval(
          statement,
          modelId,
          device,
          targetDevices,
          pollIntervalMillis,
          unloadTimeoutMillis,
          interrupted);
    } finally {
      if (interrupted.get()) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void waitForSubmittedModelLoadToSettle(
      Statement statement,
      String modelId,
      String device,
      Set<String> targetDevices,
      long loadSubmittedNanos,
      long pollIntervalMillis,
      long loadSettleTimeoutMillis,
      AtomicBoolean interrupted)
      throws SQLException {
    long settleDeadlineNanos =
        loadSubmittedNanos + TimeUnit.MILLISECONDS.toNanos(loadSettleTimeoutMillis);
    Set<String> everRegisteredDevices = new HashSet<>();
    while (true) {
      Map<String, Integer> deviceCounts =
          queryModelDeviceCounts(statement, modelId, device, targetDevices);
      everRegisteredDevices.addAll(deviceCounts.keySet());
      Set<String> runningDevices = new HashSet<>();
      boolean hasInitializingDevice = false;
      for (Map.Entry<String, Integer> entry : deviceCounts.entrySet()) {
        if (entry.getValue() > 0) {
          runningDevices.add(entry.getKey());
        } else {
          hasInitializingDevice = true;
        }
      }
      if (runningDevices.containsAll(targetDevices)) {
        LOGGER.info(
            "Submitted load of model {} completed on devices {} before cleanup unload.",
            modelId,
            targetDevices);
        return;
      }
      if (everRegisteredDevices.containsAll(targetDevices) && !hasInitializingDevice) {
        LOGGER.info(
            "Submitted load of model {} settled on devices {} (running={}) before cleanup unload.",
            modelId,
            targetDevices,
            runningDevices);
        return;
      }
      long remainingNanos = settleDeadlineNanos - System.nanoTime();
      if (remainingNanos <= 0L) {
        if (runningDevices.isEmpty()) {
          Set<String> unobservedDevices = new HashSet<>(targetDevices);
          unobservedDevices.removeAll(everRegisteredDevices);
          fail(
              "Submitted load of model "
                  + modelId
                  + " on devices "
                  + device
                  + " remained unresolved after "
                  + loadSettleTimeoutMillis
                  + " ms (current="
                  + deviceCounts
                  + ", unobserved="
                  + unobservedDevices
                  + "); UNLOAD MODEL cannot guarantee cleanup without a running pool.");
        }
        LOGGER.warn(
            "Timed out waiting for submitted load of model {} on devices {} to settle (current={}); queuing unload for registered pools.",
            modelId,
            targetDevices,
            deviceCounts);
        return;
      }
      long sleepMillis =
          Math.min(pollIntervalMillis, Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
      if (sleepUninterruptibly(sleepMillis)) {
        interrupted.set(true);
      }
    }
  }

  private static void waitForModelRemoval(
      Statement statement,
      String modelId,
      String device,
      Set<String> targetDevices,
      long pollIntervalMillis,
      long unloadTimeoutMillis,
      AtomicBoolean interrupted)
      throws SQLException {
    long unloadDeadlineNanos =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(unloadTimeoutMillis);
    while (true) {
      Map<String, Integer> deviceCounts =
          queryModelDeviceCounts(statement, modelId, device, targetDevices);
      if (deviceCounts.isEmpty()) {
        LOGGER.info("Model {} is unloaded from devices {}.", modelId, targetDevices);
        return;
      }
      long remainingNanos = unloadDeadlineNanos - System.nanoTime();
      if (remainingNanos <= 0L) {
        fail("Model " + modelId + " is still loaded on device " + device + ": " + deviceCounts);
      }
      long sleepMillis =
          Math.min(pollIntervalMillis, Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
      if (sleepUninterruptibly(sleepMillis)) {
        interrupted.set(true);
      }
    }
  }

  private static Map<String, Integer> queryModelDeviceCounts(
      Statement statement, String modelId, String device, Set<String> targetDevices)
      throws SQLException {
    Map<String, Integer> deviceCounts = new HashMap<>();
    try (final ResultSet resultSet =
        statement.executeQuery(String.format("SHOW LOADED MODELS '%s'", device))) {
      while (resultSet.next()) {
        String deviceId = resultSet.getString("DeviceId");
        String loadedModelId = resultSet.getString("ModelId");
        int count = resultSet.getInt("Count(instances)");
        LOGGER.info("Model {} found in device {}, count {}", loadedModelId, deviceId, count);
        if (loadedModelId.equals(modelId) && targetDevices.contains(deviceId)) {
          deviceCounts.put(deviceId, count);
        }
      }
    }
    return deviceCounts;
  }

  private static boolean sleepUninterruptibly(long millis) {
    if (millis <= 0L) {
      return false;
    }
    boolean interrupted = false;
    long endNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (true) {
      long remainingNanos = endNanos - System.nanoTime();
      if (remainingNanos <= 0L) {
        return interrupted;
      }
      try {
        TimeUnit.NANOSECONDS.sleep(remainingNanos);
        return interrupted;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
  }

  private static final String[] WRITE_SQL_IN_TREE =
      new String[] {
        "CREATE DATABASE root.AI",
        "CREATE TIMESERIES root.AI.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s1 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s2 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.AI.s3 WITH DATATYPE=INT64, ENCODING=RLE",
      };

  /** Prepare root.AI(s0 FLOAT, s1 DOUBLE, s2 INT32, s3 INT64) with 5760 rows of data in tree. */
  public static void prepareDataInTree() throws SQLException {
    prepareData(WRITE_SQL_IN_TREE);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 5760; i++) {
        statement.addBatch(
            String.format(
                "INSERT INTO root.AI(timestamp,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
        if ((i + 1) % 500 == 0) {
          statement.executeBatch();
          statement.clearBatch();
        }
      }
      statement.executeBatch();
    }
  }

  /** Prepare db.AI(s0 FLOAT, s1 DOUBLE, s2 INT32, s3 INT64) with 5760 rows of data in table. */
  public static void prepareDataInTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE db");
      statement.execute(
          "CREATE TABLE db.AI (s0 FLOAT FIELD, s1 DOUBLE FIELD, s2 INT32 FIELD, s3 INT64 FIELD)");
      for (int i = 0; i < 5760; i++) {
        statement.addBatch(
            String.format(
                "INSERT INTO db.AI(time,s0,s1,s2,s3) VALUES(%d,%f,%f,%d,%d)",
                i, (float) i, (double) i, i, i));
        if ((i + 1) % 500 == 0) {
          statement.executeBatch();
          statement.clearBatch();
        }
      }
      statement.executeBatch();
    }
  }

  /** Prepare db.AI2(s0 FLOAT,...) with 2880 rows of data in table. */
  public static void prepareDataInTable2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE db");
      statement.execute(
          "CREATE TABLE db.AI2 (s0 FLOAT FIELD, s1 DOUBLE FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD, s5 DOUBLE FIELD, s6 INT32 FIELD, s7 INT64 FIELD, s8 FLOAT FIELD, s9 DOUBLE FIELD)");
      for (int i = 0; i < 2880; i++) {
        statement.addBatch(
            String.format(
                "INSERT INTO db.AI2(time,s0,s1,s2,s3,s4,s5,s6,s7,s8,s9) VALUES(%d,%f,%f,%d,%d,%f,%f,%d,%d,%f,%f)",
                i,
                (float) i,
                (double) i,
                i,
                i,
                (float) (i * 2),
                (double) (i * 2),
                i * 2,
                i * 2,
                (float) (i * 3),
                (double) (i * 3)));
        if ((i + 1) % 500 == 0) {
          statement.executeBatch();
          statement.clearBatch();
        }
      }
      statement.executeBatch();
    }
  }

  public static class FakeModelInfo {

    private final String modelId;
    private final String modelType;
    private final String category;
    private final String state;

    public FakeModelInfo(String modelId, String modelType, String category, String state) {
      this.modelId = modelId;
      this.modelType = modelType;
      this.category = category;
      this.state = state;
    }

    public String getModelId() {
      return modelId;
    }

    public String getModelType() {
      return modelType;
    }

    public String getCategory() {
      return category;
    }

    public String getState() {
      return state;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FakeModelInfo modelInfo = (FakeModelInfo) o;
      return Objects.equals(modelId, modelInfo.modelId)
          && Objects.equals(modelType, modelInfo.modelType)
          && Objects.equals(category, modelInfo.category)
          && Objects.equals(state, modelInfo.state);
    }

    @Override
    public int hashCode() {
      return Objects.hash(modelId, modelType, category, state);
    }

    @Override
    public String toString() {
      return "FakeModelInfo{"
          + "modelId='"
          + modelId
          + '\''
          + ", modelType='"
          + modelType
          + '\''
          + ", category='"
          + category
          + '\''
          + ", state='"
          + state
          + '\''
          + '}';
    }
  }
}
