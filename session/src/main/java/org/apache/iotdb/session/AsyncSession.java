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
package org.apache.iotdb.session;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionUtils.TimeOutCanceller;
import org.apache.iotdb.session.pool.SessionThreadPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncSession {
  private static final Logger logger = LoggerFactory.getLogger(AsyncSession.class);
  private final SessionThreadPool threadPool;

  public AsyncSession() {
    threadPool = new SessionThreadPool();
  }

  public AsyncSession(int threadPoolSize, int blockingQueueSize) {
    threadPool = new SessionThreadPool(threadPoolSize, blockingQueueSize);
  }

  /**
   * insert data in one row asynchronously. if you want improve your performance,
   * please use insertRecords method or insertTablet method
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecord(String deviceId, long time,
      List<String> measurements, List<TSDataType> types, List<Object> values, long timeout,
      SixInputConsumer<String, Long, List<String>, List<TSDataType>, List<Object>, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertRecord(deviceId, time, measurements, types, values);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler)
        .exceptionally(e -> {
          if (callback == null) {
            logger.error("Error occurred when inserting record, device ID: {}, time: {}. ",
                deviceId, time, e);
          } else {
            callback.apply(deviceId, time, measurements, types, values, e);
          }
          return -1;
        });
  }

  /**
   * insert data in one row asynchronously. if you want improve your performance,
   * please use insertRecords method or insertTablet method
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecord(String deviceId, long time,
      List<String> measurements, List<String> values, long timeout,
      FiveInputConsumer<String, Long, List<String>, List<String>, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertRecord(deviceId, time, measurements, values);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler)
        .exceptionally(e -> {
          if (callback == null) {
            logger.error("Error occurred when inserting record, device ID: {}, time: {}. ",
                deviceId, time, e);
          } else {
            callback.apply(deviceId, time, measurements, values, e);
          }
          return -1;
        });
  }

  /**
   * Insert multiple rows in asynchronous way. This method is just like jdbc executeBatch,
   * we pack some insert request in batch and send them to server. If you want improve your
   * performance, please see insertTablet method.
   * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList, long timeout,
      SixInputConsumer<List<String>, List<Long>, List<List<String>>, List<List<TSDataType>>, List<List<Object>>, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler).
            exceptionally(exception ->
            {
              if (callback == null) {
                logger.error("Error occurred when inserting records, device ID: {}, " +
                        "time list of length: {}, starting from {}.",
                    deviceIds.get(0), times.size(), times.get(0), exception);
              } else {
                callback.apply(deviceIds, times, measurementsList, typesList, valuesList, exception);
              }
              return -1;
            });
  }

  /**
   * Insert multiple rows in asynchronous way. This method is just like jdbc executeBatch,
   * we pack some insert request in batch and send them to server. If you want improve your
   * performance, please see insertTablet method.
   * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList, long timeout,
      FiveInputConsumer<List<String>, List<Long>, List<List<String>>,
      List<List<String>>, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertRecords(deviceIds, times, measurementsList, valuesList);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler)
        .exceptionally(e -> {
          if (callback == null) {
            logger.error("Error occurred when inserting records, device ID: {}, " +
                    "time list of length: {}, starting from {}.",
                deviceIds.get(0), times.size(), times.get(0), e);
          } else {
            callback.apply(deviceIds, times, measurementsList, valuesList, e);
          }
          return -1;
        });
  }

  /**
   * insert a Tablet asynchronously
   *
   * @param tablet   data batch
   * @param sorted   whether times in Tablet are in ascending order
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   * @return async CompletableFuture
   */
  public CompletableFuture<Integer> asyncInsertTablet(Tablet tablet, boolean sorted,
      long timeout, BiConsumer<Tablet, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertTablet(tablet, sorted);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler)
        .exceptionally(e -> {
          if (callback == null) {
            logger.error("Error occurred when inserting tablet, device ID: {}, " +
                    "time list of length: {}, starting from {}.",
                tablet.deviceId, tablet.timestamps.length, tablet.timestamps[0], e);
          } else {
            callback.accept(tablet, e);
          }
          return -1;
        });
  }

  /**
   * insert the data of several devices asynchronously. Given a device, for each timestamp,
   * the number of measurements is the same.
   *
   * @param tablets  data batch in multiple device
   * @param sorted   whether times in each Tablet are in ascending order
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided failure callback, set to null if user does not specify.
   */
  public CompletableFuture<Integer> asyncInsertTablets(Map<String, Tablet> tablets, boolean sorted,
      long timeout, BiConsumer<Map<String, Tablet>, Throwable> callback) {
    CompletableFuture<Integer> asyncRun = CompletableFuture.supplyAsync(() -> {
      try {
        insertTablets(tablets, sorted);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      return 0;
    }, threadPool.getThreadPool());

    return asyncRun
        .applyToEitherAsync(orTimeout(timeout, TimeUnit.MILLISECONDS), this::successHandler)
        .exceptionally(e -> {
          if ((callback == null)) {
            logger.error("Error occurred when inserting tablets, tablet list size: {}",
                tablets.size(), e);
          } else {
            callback.accept(tablets, e);
          }
          return -1;
        });
  }

  public abstract void insertRecord(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException;

  public abstract void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) throws IoTDBConnectionException, StatementExecutionException;

  public abstract void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  private static <T> CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
    if (unit == null)
      throw new NullPointerException();
    CompletableFuture<T> promise = new CompletableFuture<>();
    promise.whenComplete(new TimeOutCanceller(
        SessionUtils.Delayer.delay(new SessionUtils.Timeout(promise), timeout, unit)));
    return promise;
  }

  private int successHandler(Integer integer) {
    return 0;
  }

  @FunctionalInterface
  private interface FiveInputConsumer<First, Second, Third, Fourth, Fifth> {
    void apply(First one, Second two, Third three, Fourth four, Fifth five);
  }

  @FunctionalInterface
  private interface SixInputConsumer<First, Second, Third, Fourth, Fifth, Sixth> {
    void apply(First one, Second two, Third three, Fourth four, Fifth five, Sixth six);
  }
}
