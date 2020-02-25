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
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.junit.Assert;
import org.junit.Test;

public class AggregateResultTest {

  @Test
  public void avgAggrResultTest() throws QueryProcessException {
    AggregateResult avgAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.AVG, TSDataType.DOUBLE);
    AggregateResult avgAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.AVG, TSDataType.DOUBLE);

    Statistics statistics = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics.update(1l,1d);

    avgAggrResult1.updateResultFromStatistics(statistics);
    avgAggrResult2.updateResultFromStatistics(statistics);
    avgAggrResult1.merge(avgAggrResult2);
    Assert.assertEquals(1d, (double)avgAggrResult1.getResult(), 0.01);
  }

  @Test
  public void maxValueAggrResultTest() throws QueryProcessException {
    AggregateResult maxValueAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.MAX_VALUE, TSDataType.DOUBLE);
    AggregateResult maxValueAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.MAX_VALUE, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(1l,2d);

    maxValueAggrResult1.updateResultFromStatistics(statistics1);
    maxValueAggrResult2.updateResultFromStatistics(statistics2);
    maxValueAggrResult1.merge(maxValueAggrResult2);

    Assert.assertEquals(2d, (double)maxValueAggrResult1.getResult(), 0.01);
  }

  @Test
  public void maxTimeAggrResultTest() throws QueryProcessException {
    AggregateResult maxTimeAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.MAX_TIME, TSDataType.DOUBLE);
    AggregateResult maxTimeAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.MAX_TIME, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(2l,1d);

    maxTimeAggrResult1.updateResultFromStatistics(statistics1);
    maxTimeAggrResult2.updateResultFromStatistics(statistics2);
    maxTimeAggrResult1.merge(maxTimeAggrResult2);

    Assert.assertEquals(2l, (long)maxTimeAggrResult1.getResult(), 0.01);
  }

  @Test
  public void minValueAggrResultTest() throws QueryProcessException {
    AggregateResult minValueAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.MIN_VALUE, TSDataType.DOUBLE);
    AggregateResult minValueAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.MIN_VALUE, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(1l,2d);

    minValueAggrResult1.updateResultFromStatistics(statistics1);
    minValueAggrResult2.updateResultFromStatistics(statistics2);
    minValueAggrResult1.merge(minValueAggrResult2);

    Assert.assertEquals(1d, (double)minValueAggrResult1.getResult(), 0.01);
  }

  @Test
  public void minTimeAggrResultTest() throws QueryProcessException {
    AggregateResult minTimeAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.DOUBLE);
    AggregateResult minTimeAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(2l,1d);

    minTimeAggrResult1.updateResultFromStatistics(statistics1);
    minTimeAggrResult2.updateResultFromStatistics(statistics2);
    minTimeAggrResult1.merge(minTimeAggrResult2);

    Assert.assertEquals(1l, (long)minTimeAggrResult1.getResult(), 0.01);
  }

  @Test
  public void countAggrResultTest() throws QueryProcessException {
    AggregateResult countAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.COUNT, TSDataType.INT64);
    AggregateResult countAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.COUNT, TSDataType.INT64);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.INT64);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.INT64);
    statistics1.update(1l, 1l);
    statistics2.update(1l,1l);

    countAggrResult1.updateResultFromStatistics(statistics1);
    countAggrResult2.updateResultFromStatistics(statistics2);
    countAggrResult1.merge(countAggrResult2);

    Assert.assertEquals(2, (long)countAggrResult1.getResult(), 0.01);
  }

  @Test
  public void sumAggrResultTest() throws QueryProcessException {
    AggregateResult sumAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.SUM, TSDataType.DOUBLE);
    AggregateResult sumAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.SUM, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(1l,2d);

    sumAggrResult1.updateResultFromStatistics(statistics1);
    sumAggrResult2.updateResultFromStatistics(statistics2);
    sumAggrResult1.merge(sumAggrResult2);

    Assert.assertEquals(3d, (double)sumAggrResult1.getResult(), 0.01);
  }

  @Test
  public void firstValueAggrResultTest() throws QueryProcessException {
    AggregateResult firstValueAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.FIRST_VALUE, TSDataType.DOUBLE);
    AggregateResult firstValueAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.FIRST_VALUE, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(2l,2d);

    firstValueAggrResult1.updateResultFromStatistics(statistics1);
    firstValueAggrResult2.updateResultFromStatistics(statistics2);
    firstValueAggrResult1.merge(firstValueAggrResult2);

    Assert.assertEquals(1d, (double)firstValueAggrResult1.getResult(), 0.01);
  }

  @Test
  public void lastValueAggrResultTest() throws QueryProcessException {
    AggregateResult lastValueAggrResult1 = AggreResultFactory.getAggrResultByName(SQLConstant.LAST_VALUE, TSDataType.DOUBLE);
    AggregateResult lastValueAggrResult2 = AggreResultFactory.getAggrResultByName(SQLConstant.LAST_VALUE, TSDataType.DOUBLE);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.DOUBLE);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics1.update(1l, 1d);
    statistics2.update(2l,2d);

    lastValueAggrResult1.updateResultFromStatistics(statistics1);
    lastValueAggrResult2.updateResultFromStatistics(statistics2);
    lastValueAggrResult1.merge(lastValueAggrResult2);

    Assert.assertEquals(2d, (double)lastValueAggrResult1.getResult(), 0.01);
  }


}
