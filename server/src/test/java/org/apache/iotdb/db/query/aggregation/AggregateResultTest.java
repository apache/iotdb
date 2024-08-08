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

package org.apache.iotdb.db.query.aggregation;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Unit tests of AggregateResult without desc aggregate result. */
public class AggregateResultTest {

  @Test
  public void avgAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult avgAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.AVG, TSDataType.DOUBLE, true);
    AggregateResult avgAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.AVG, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics1.update(2L, 1d);
    statistics2.update(1L, 2d);

    avgAggrResult1.updateResultFromStatistics(statistics1);
    avgAggrResult2.updateResultFromStatistics(statistics2);
    avgAggrResult1.merge(avgAggrResult2);
    Assert.assertEquals(1.333d, (double) avgAggrResult1.getResult(), 0.01);
    Assert.assertEquals(3, ((AvgAggrResult) avgAggrResult1).getCnt());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    avgAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(1.333d, (double) result.getResult(), 0.01);
  }

  @Test
  public void maxValueAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult maxValueAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MAX_VALUE, TSDataType.DOUBLE, true);
    AggregateResult maxValueAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MAX_VALUE, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(1L, 2d);

    maxValueAggrResult1.updateResultFromStatistics(statistics1);
    maxValueAggrResult2.updateResultFromStatistics(statistics2);
    maxValueAggrResult1.merge(maxValueAggrResult2);

    Assert.assertEquals(2d, (double) maxValueAggrResult1.getResult(), 0.01);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    maxValueAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(2d, (double) result.getResult(), 0.01);
  }

  @Test
  public void maxTimeAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult maxTimeAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MAX_TIME, TSDataType.DOUBLE, true);
    AggregateResult maxTimeAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MAX_TIME, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(2L, 1d);

    maxTimeAggrResult1.updateResultFromStatistics(statistics1);
    maxTimeAggrResult2.updateResultFromStatistics(statistics2);
    maxTimeAggrResult1.merge(maxTimeAggrResult2);

    Assert.assertEquals(2L, (long) maxTimeAggrResult1.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    maxTimeAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(2L, (long) result.getResult());
  }

  @Test
  public void minValueAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult minValueAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_VALUE, TSDataType.DOUBLE, true);
    AggregateResult minValueAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_VALUE, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(1L, 2d);

    minValueAggrResult1.updateResultFromStatistics(statistics1);
    minValueAggrResult2.updateResultFromStatistics(statistics2);
    minValueAggrResult1.merge(minValueAggrResult2);

    Assert.assertEquals(1d, (double) minValueAggrResult1.getResult(), 0.01);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    minValueAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(1d, (double) result.getResult(), 0.01);
  }

  @Test
  public void ExtremeAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult extremeAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.EXTREME, TSDataType.DOUBLE, true);
    AggregateResult extremeAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.EXTREME, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(1L, -2d);

    extremeAggrResult1.updateResultFromStatistics(statistics1);
    extremeAggrResult2.updateResultFromStatistics(statistics2);
    extremeAggrResult1.merge(extremeAggrResult2);

    Assert.assertEquals(-2d, (double) extremeAggrResult1.getResult(), 0.01);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    extremeAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(-2d, (double) result.getResult(), 0.01);
  }

  @Test
  public void minTimeAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult finalResult =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.DOUBLE, true);
    AggregateResult minTimeAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.DOUBLE, true);
    AggregateResult minTimeAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(10L, 1d);
    statistics2.update(2L, 1d);

    minTimeAggrResult1.updateResultFromStatistics(statistics1);
    minTimeAggrResult2.updateResultFromStatistics(statistics2);
    finalResult.merge(minTimeAggrResult1);
    finalResult.merge(minTimeAggrResult2);

    Assert.assertEquals(2L, (long) finalResult.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    finalResult.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(2L, (long) result.getResult());
  }

  @Test
  public void countAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult countAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.COUNT, TSDataType.INT64, true);
    AggregateResult countAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.COUNT, TSDataType.INT64, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.INT64);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.INT64);
    statistics1.update(1L, 1L);
    statistics2.update(1L, 1L);

    countAggrResult1.updateResultFromStatistics(statistics1);
    countAggrResult2.updateResultFromStatistics(statistics2);
    countAggrResult1.merge(countAggrResult2);

    Assert.assertEquals(2L, (long) countAggrResult1.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    countAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(2L, (long) result.getResult());
  }

  @Test
  public void sumAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult sumAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.SUM, TSDataType.INT32, true);
    AggregateResult sumAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.SUM, TSDataType.INT32, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.INT32);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.INT32);
    statistics1.update(1L, 1);
    statistics2.update(1L, 2);

    sumAggrResult1.updateResultFromStatistics(statistics1);
    sumAggrResult2.updateResultFromStatistics(statistics2);
    sumAggrResult1.merge(sumAggrResult2);

    Assert.assertEquals(3.0, sumAggrResult1.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    sumAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(3.0, result.getResult());
  }

  @Test
  public void firstValueAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult firstValueAggrResult1 =
        AggregateResultFactory.getAggrResultByName(
            SQLConstant.FIRST_VALUE, TSDataType.DOUBLE, true);
    AggregateResult firstValueAggrResult2 =
        AggregateResultFactory.getAggrResultByName(
            SQLConstant.FIRST_VALUE, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(2L, 2d);

    firstValueAggrResult1.updateResultFromStatistics(statistics1);
    firstValueAggrResult2.updateResultFromStatistics(statistics2);
    firstValueAggrResult1.merge(firstValueAggrResult2);

    Assert.assertEquals(1d, (double) firstValueAggrResult1.getResult(), 0.01);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    firstValueAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(1d, (double) result.getResult(), 0.01);
  }

  @Test
  public void lastValueAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult lastValueAggrResult1 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.LAST_VALUE, TSDataType.DOUBLE, true);
    AggregateResult lastValueAggrResult2 =
        AggregateResultFactory.getAggrResultByName(SQLConstant.LAST_VALUE, TSDataType.DOUBLE, true);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1L, 1d);
    statistics2.update(2L, 2d);

    lastValueAggrResult1.updateResultFromStatistics(statistics1);
    lastValueAggrResult2.updateResultFromStatistics(statistics2);
    lastValueAggrResult1.merge(lastValueAggrResult2);

    Assert.assertEquals(2d, (double) lastValueAggrResult1.getResult(), 0.01);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    lastValueAggrResult1.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(2d, (double) result.getResult(), 0.01);
  }
}
