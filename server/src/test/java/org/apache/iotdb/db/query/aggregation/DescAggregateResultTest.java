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

/** Unit tests of desc aggregate result. */
public class DescAggregateResultTest {

  //  @Test
  //  public void validityMergeTest() throws QueryProcessException, IOException {
  //    AggregateResult ValidityAggrResult =
  //        AggregateResultFactory.getAggrResultByName(SQLConstant.VALIDITY, TSDataType.DOUBLE,
  // true);
  //
  //    System.out.println(Runtime.getRuntime().freeMemory() / 1024 / 1024);
  //
  //    BatchData batchData = BatchDataFactory.createBatchData(TSDataType.DOUBLE, true, true);
  //    BatchData batchData2 = BatchDataFactory.createBatchData(TSDataType.DOUBLE, true, true);
  //
  //    for (int i = 0; i < 3000; i++) {
  //      batchData.putDouble(1623311071000L - 3000 + i, 0d);
  //    }
  //    batchData.putDouble(1623311071000L, 0d);
  //    batchData.putDouble(1623312051000L, 2d);
  //    batchData.putDouble(1623312053000L, 2d);
  //    batchData.putDouble(1623312055000L, 2d);
  //    batchData.putDouble(1623312057000L, 2d);
  //    for (int i = 0; i < 7024; i++) {
  //      batchData2.putDouble(1623312057000L + i, 0d);
  //    }
  //    batchData.resetBatchData();
  //    batchData2.resetBatchData();
  //    IBatchDataIterator it = batchData.getBatchDataIterator();
  //    IBatchDataIterator it2 = batchData2.getBatchDataIterator();
  //    ValidityAggrResult.updateResultFromPageData(it);
  //    it.reset();
  //    ValidityAggrResult.updateResultFromPageData(it2);
  //  }
}
