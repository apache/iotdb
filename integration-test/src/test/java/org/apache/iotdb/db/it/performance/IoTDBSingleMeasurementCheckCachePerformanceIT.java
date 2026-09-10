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

package org.apache.iotdb.db.it.performance;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Ignore
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSingleMeasurementCheckCachePerformanceIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSingleMeasurementCheckCachePerformanceIT.class);

  private static final ExperimentGroup[] EXPERIMENT_GROUPS = {
    new ExperimentGroup(1_000, 500),
    new ExperimentGroup(2_000, 2_000),
    new ExperimentGroup(4_000, 8_000)
  };
  private static final int CACHE_DISABLED_SIZE = 0;
  private static final int REPEAT_COUNT = 3;
  private static final int BATCH_COUNT = 800;
  private static final int ROWS_PER_BATCH = 100;
  private static final String DEVICE = "root.sg_cache_perf.d1";

  @Test
  public void testEndToEndWritePerformanceWithDifferentSingleMeasurementCheckCacheSizes()
      throws Exception {
    for (ExperimentGroup experimentGroup : EXPERIMENT_GROUPS) {
      long totalDisabledCost = 0;
      long totalEnabledCost = 0;
      for (int repeatIndex = 0; repeatIndex < REPEAT_COUNT; repeatIndex++) {
        long disabledCost =
            runWritePerformanceExperiment(
                new Experiment(experimentGroup.measurementCount, CACHE_DISABLED_SIZE));
        long enabledCost =
            runWritePerformanceExperiment(
                new Experiment(experimentGroup.measurementCount, experimentGroup.cacheSize));
        totalDisabledCost += disabledCost;
        totalEnabledCost += enabledCost;
        LOGGER.info(
            "End-to-end write cost repeat {}/{} with measurementCount={}, cache disabled: {} ms, "
                + "cacheSize={} (cacheSize {} measurementCount): {} ms, enabled/disabled ratio: {}",
            repeatIndex + 1,
            REPEAT_COUNT,
            experimentGroup.measurementCount,
            disabledCost / 1_000_000,
            experimentGroup.cacheSize,
            experimentGroup.cacheSizeRelation(),
            enabledCost / 1_000_000,
            String.format("%.3f", (double) enabledCost / disabledCost));
      }
      long averageDisabledCost = totalDisabledCost / REPEAT_COUNT;
      long averageEnabledCost = totalEnabledCost / REPEAT_COUNT;
      LOGGER.info(
          "Average end-to-end write cost after {} repeats with measurementCount={}, cache disabled: "
              + "{} ms, cacheSize={} (cacheSize {} measurementCount): {} ms, enabled/disabled "
              + "ratio: {}",
          REPEAT_COUNT,
          experimentGroup.measurementCount,
          averageDisabledCost / 1_000_000,
          experimentGroup.cacheSize,
          experimentGroup.cacheSizeRelation(),
          averageEnabledCost / 1_000_000,
          String.format("%.3f", (double) averageEnabledCost / averageDisabledCost));
      Assert.assertTrue(totalDisabledCost > 0);
      Assert.assertTrue(totalEnabledCost > 0);
    }
  }

  private long runWritePerformanceExperiment(Experiment experiment) throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSingleMeasurementCheckCacheSize(experiment.cacheSize)
        .setAutoCreateSchemaEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();
    try {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        createTimeseries(session, experiment.measurementCount);
        long cost = executeWriteWorkload(session, experiment.measurementCount);
        assertRowCount(session);
        return cost;
      }
    } finally {
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  private void createTimeseries(ISession session, int measurementCount)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("CREATE DATABASE root.sg_cache_perf");
    for (int i = 0; i < measurementCount; i++) {
      session.executeNonQueryStatement(
          "CREATE TIMESERIES "
              + DEVICE
              + ".`sensor+"
              + i
              + "` WITH DATATYPE=INT64, ENCODING=PLAIN");
    }
  }

  private long executeWriteWorkload(ISession session, int measurementCount)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>(measurementCount);
    List<TSDataType> types = new ArrayList<>(measurementCount);
    List<Object> values = new ArrayList<>(measurementCount);
    for (int i = 0; i < measurementCount; i++) {
      measurements.add("`sensor+" + i + "`");
      types.add(TSDataType.INT64);
      values.add((long) i);
    }

    long startTime = System.nanoTime();
    for (int batchIndex = 0; batchIndex < BATCH_COUNT; batchIndex++) {
      List<Long> timestamps = new ArrayList<>(ROWS_PER_BATCH);
      List<List<String>> measurementsList = new ArrayList<>(ROWS_PER_BATCH);
      List<List<TSDataType>> typesList = new ArrayList<>(ROWS_PER_BATCH);
      List<List<Object>> valuesList = new ArrayList<>(ROWS_PER_BATCH);
      for (int rowIndex = 0; rowIndex < ROWS_PER_BATCH; rowIndex++) {
        timestamps.add((long) batchIndex * ROWS_PER_BATCH + rowIndex);
        measurementsList.add(measurements);
        typesList.add(types);
        valuesList.add(values);
      }
      session.insertRecordsOfOneDevice(DEVICE, timestamps, measurementsList, typesList, valuesList);
    }
    return System.nanoTime() - startTime;
  }

  private void assertRowCount(ISession session)
      throws IoTDBConnectionException, StatementExecutionException {
    try (org.apache.iotdb.isession.SessionDataSet dataSet =
        session.executeQueryStatement("SELECT COUNT(`sensor+0`) FROM " + DEVICE)) {
      Assert.assertTrue(dataSet.hasNext());
      Assert.assertEquals(
          (long) BATCH_COUNT * ROWS_PER_BATCH, dataSet.next().getFields().get(0).getLongV());
      Assert.assertFalse(dataSet.hasNext());
    }
  }

  private static class ExperimentGroup {

    private final int measurementCount;
    private final int cacheSize;

    private ExperimentGroup(int measurementCount, int cacheSize) {
      this.measurementCount = measurementCount;
      this.cacheSize = cacheSize;
    }

    private String cacheSizeRelation() {
      if (cacheSize < measurementCount) {
        return "<";
      }
      if (cacheSize == measurementCount) {
        return "=";
      }
      return ">";
    }
  }

  private static class Experiment {

    private final int measurementCount;
    private final int cacheSize;

    private Experiment(int measurementCount, int cacheSize) {
      this.measurementCount = measurementCount;
      this.cacheSize = cacheSize;
    }
  }
}
