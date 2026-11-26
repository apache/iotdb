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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBAlignedTVListBitMapPerformanceIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBAlignedTVListBitMapPerformanceIT.class);

  private static final String ENABLED_PROPERTY = "iotdb.aligned.bitmap.perf.enabled";
  private static final String MEASUREMENT_COUNT_PROPERTY =
      "iotdb.aligned.bitmap.perf.measurement.count";
  private static final String WARMUP_BATCH_COUNT_PROPERTY =
      "iotdb.aligned.bitmap.perf.warmup.batch.count";
  private static final String BATCH_COUNT_PROPERTY = "iotdb.aligned.bitmap.perf.batch.count";
  private static final String ROUND_COUNT_PROPERTY = "iotdb.aligned.bitmap.perf.round.count";

  private static final int ROWS_PER_BATCH = 64;
  private static final int MEASUREMENT_COUNT = Integer.getInteger(MEASUREMENT_COUNT_PROPERTY, 256);
  private static final int WARMUP_BATCH_COUNT = Integer.getInteger(WARMUP_BATCH_COUNT_PROPERTY, 50);
  private static final int BATCH_COUNT = Integer.getInteger(BATCH_COUNT_PROPERTY, 100);
  private static final int ROUND_COUNT = Integer.getInteger(ROUND_COUNT_PROPERTY, 5);
  private static final String DEVICE = "root.aligned_bitmap_performance.d1";

  @BeforeClass
  public static void setUp() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean(ENABLED_PROPERTY));
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPrimitiveArraySize(ROWS_PER_BATCH)
        .setMemtableSizeThreshold(512L * 1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNullHeavyAlignedTabletWritePerformance() throws Exception {
    Assert.assertTrue(MEASUREMENT_COUNT > 0);
    Assert.assertTrue(WARMUP_BATCH_COUNT >= 0);
    Assert.assertTrue(BATCH_COUNT > 0);
    Assert.assertTrue(ROUND_COUNT > 0);

    Tablet tablet = createTablet();
    long nextTimestamp = 0;
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      nextTimestamp = insertBatches(session, tablet, nextTimestamp, WARMUP_BATCH_COUNT);

      long[] elapsedNanos = new long[ROUND_COUNT];
      for (int round = 0; round < ROUND_COUNT; round++) {
        long startTime = System.nanoTime();
        nextTimestamp = insertBatches(session, tablet, nextTimestamp, BATCH_COUNT);
        elapsedNanos[round] = System.nanoTime() - startTime;
      }

      long expectedBatchCount = WARMUP_BATCH_COUNT + (long) BATCH_COUNT * ROUND_COUNT;
      assertFirstMeasurementCount(session, expectedBatchCount * (ROWS_PER_BATCH - 1L));
      Arrays.sort(elapsedNanos);
      double medianNanos = elapsedNanos[elapsedNanos.length / 2];
      double pointsPerSecond =
          (double) BATCH_COUNT * ROWS_PER_BATCH * MEASUREMENT_COUNT * 1_000_000_000L / medianNanos;
      LOGGER.info(
          "AlignedTVList bitmap write benchmark: measurements={}, rows/batch={}, warmup batches={}, "
              + "batches/round={}, rounds={}, median={} ms/round, throughput={} points/s",
          MEASUREMENT_COUNT,
          ROWS_PER_BATCH,
          WARMUP_BATCH_COUNT,
          BATCH_COUNT,
          ROUND_COUNT,
          String.format("%.3f", medianNanos / 1_000_000.0),
          String.format("%.0f", pointsPerSecond));
    }
  }

  private static Tablet createTablet() {
    List<IMeasurementSchema> schemas = new ArrayList<>(MEASUREMENT_COUNT);
    for (int i = 0; i < MEASUREMENT_COUNT; i++) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT64));
    }
    Tablet tablet = new Tablet(DEVICE, schemas, ROWS_PER_BATCH);
    tablet.initBitMaps();
    Object[] values = tablet.getValues();
    BitMap[] bitMaps = tablet.getBitMaps();
    for (int column = 0; column < MEASUREMENT_COUNT; column++) {
      long[] columnValues = (long[]) values[column];
      Arrays.fill(columnValues, column);
      bitMaps[column] = new BitMap(ROWS_PER_BATCH);
      bitMaps[column].mark(column % ROWS_PER_BATCH);
    }
    tablet.setRowSize(ROWS_PER_BATCH);
    return tablet;
  }

  private static long insertBatches(
      ISession session, Tablet tablet, long nextTimestamp, int batchCount) throws Exception {
    long[] timestamps = tablet.getTimestamps();
    for (int batch = 0; batch < batchCount; batch++) {
      for (int row = 0; row < ROWS_PER_BATCH; row++) {
        timestamps[row] = nextTimestamp++;
      }
      session.insertAlignedTablet(tablet);
    }
    return nextTimestamp;
  }

  private static void assertFirstMeasurementCount(ISession session, long expectedCount)
      throws Exception {
    try (org.apache.iotdb.isession.SessionDataSet dataSet =
        session.executeQueryStatement("SELECT COUNT(s0) FROM " + DEVICE)) {
      Assert.assertTrue(dataSet.hasNext());
      Assert.assertEquals(expectedCount, dataSet.next().getFields().get(0).getLongV());
      Assert.assertFalse(dataSet.hasNext());
    }
  }
}
