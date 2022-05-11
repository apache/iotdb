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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class BatchDataTest {

  @Test
  public void testInt() {
    BatchData batchData = new BatchData(TSDataType.INT32);
    assertTrue(batchData.isEmpty());
    int value = 0;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, value);
      value++;
    }
    assertEquals(TSDataType.INT32, batchData.getDataType());
    int res = 0;
    long time = 0;
    while (batchData.hasCurrent()) {
      assertEquals(time, batchData.currentTime());
      assertEquals(res, (int) batchData.currentValue());
      assertEquals(res, batchData.currentTsPrimitiveType().getInt());
      batchData.next();
      res++;
      time++;
    }
    batchData.resetBatchData();

    IPointReader reader = batchData.getBatchDataIterator();
    try {
      res = 0;
      time = 0;
      while (reader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = reader.nextTimeValuePair();
        assertEquals(time, timeValuePair.getTimestamp());
        assertEquals(res, timeValuePair.getValue().getInt());
        res++;
        time++;
      }
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testSignal() {
    BatchData batchData = SignalBatchData.getInstance();
    try {
      batchData.hasCurrent();
    } catch (UnsupportedOperationException e) {
      return;
    }
    fail();
  }
}
