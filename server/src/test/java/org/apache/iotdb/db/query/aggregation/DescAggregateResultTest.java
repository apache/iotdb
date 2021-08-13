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
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Unit tests of desc aggregate result. */
public class DescAggregateResultTest {

  @Test
  public void maxTimeDescAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult maxTimeDescAggrResult =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MAX_TIME, TSDataType.FLOAT, false);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.FLOAT);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.FLOAT);
    statistics1.update(10L, 10.0f);
    statistics2.update(1L, 1.0f);

    maxTimeDescAggrResult.updateResultFromStatistics(statistics1);
    Assert.assertEquals(10L, (long) maxTimeDescAggrResult.getResult());
    maxTimeDescAggrResult.updateResultFromStatistics(statistics2);
    Assert.assertEquals(10L, (long) maxTimeDescAggrResult.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    maxTimeDescAggrResult.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(10L, (long) result.getResult());
  }

  @Test
  public void minTimeDescAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult minTimeDescAggrResult =
        AggregateResultFactory.getAggrResultByName(SQLConstant.MIN_TIME, TSDataType.FLOAT, false);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.FLOAT);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.FLOAT);
    statistics1.update(10L, 10.0f);
    statistics2.update(1L, 1.0f);

    minTimeDescAggrResult.updateResultFromStatistics(statistics1);
    Assert.assertEquals(10L, (long) minTimeDescAggrResult.getResult());
    minTimeDescAggrResult.updateResultFromStatistics(statistics2);
    Assert.assertEquals(1L, (long) minTimeDescAggrResult.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    minTimeDescAggrResult.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(1L, (long) result.getResult());
  }

  @Test
  public void firstValueDescAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult firstValueDescAggrResult =
        AggregateResultFactory.getAggrResultByName(
            SQLConstant.FIRST_VALUE, TSDataType.BOOLEAN, false);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.BOOLEAN);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.BOOLEAN);
    statistics1.update(10L, true);
    statistics2.update(1L, false);

    firstValueDescAggrResult.updateResultFromStatistics(statistics1);
    Assert.assertEquals(true, firstValueDescAggrResult.getResult());
    firstValueDescAggrResult.updateResultFromStatistics(statistics2);
    Assert.assertEquals(false, firstValueDescAggrResult.getResult());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    firstValueDescAggrResult.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals(false, result.getResult());
  }

  @Test
  public void lastValueDescAggrResultTest() throws QueryProcessException, IOException {
    AggregateResult lastValueDescAggrResult =
        AggregateResultFactory.getAggrResultByName(SQLConstant.LAST_VALUE, TSDataType.TEXT, false);

    Statistics statistics1 = Statistics.getStatsByType(TSDataType.TEXT);
    Statistics statistics2 = Statistics.getStatsByType(TSDataType.TEXT);
    statistics1.update(10L, new Binary("last"));
    statistics2.update(1L, new Binary("first"));

    lastValueDescAggrResult.updateResultFromStatistics(statistics1);
    Assert.assertEquals("last", String.valueOf(lastValueDescAggrResult.getResult()));
    lastValueDescAggrResult.updateResultFromStatistics(statistics2);
    Assert.assertEquals("last", String.valueOf(lastValueDescAggrResult.getResult()));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    lastValueDescAggrResult.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    AggregateResult result = AggregateResult.deserializeFrom(byteBuffer);
    Assert.assertEquals("last", String.valueOf(result.getResult()));
  }
}
