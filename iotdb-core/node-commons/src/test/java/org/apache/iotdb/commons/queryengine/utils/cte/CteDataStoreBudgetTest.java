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

package org.apache.iotdb.commons.queryengine.utils.cte;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CteDataStoreBudgetTest {
  private TsBlock block(int rows) {
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.INT32));
    for (int i = 0; i < rows; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeInt(i);
      builder.declarePosition();
    }
    return builder.build();
  }

  @Test
  public void testIndependentBudgetsAndExactBoundaries() {
    TsBlock one = block(1);
    long bytes = one.getRetainedSizeInBytes();
    CteDataStore first = new CteDataStore(null, List.of(0), bytes * 2, 2);
    CteDataStore second = new CteDataStore(null, List.of(0), bytes, 1);
    assertEquals(CteDataStore.AddResult.SUCCESS, first.tryAddTsBlock(one));
    assertEquals(CteDataStore.AddResult.SUCCESS, first.tryAddTsBlock(one));
    assertEquals(CteDataStore.AddResult.BYTES_EXCEEDED, first.tryAddTsBlock(one));
    assertEquals(CteDataStore.AddResult.SUCCESS, second.tryAddTsBlock(one));
    CteDataStore rowsLimited = new CteDataStore(null, List.of(0), Long.MAX_VALUE, 1);
    assertEquals(CteDataStore.AddResult.ROWS_EXCEEDED, rowsLimited.tryAddTsBlock(block(2)));
    first.clear();
    assertEquals(CteDataStore.AddResult.SUCCESS, first.tryAddTsBlock(one));
  }

  @Test
  public void testExtraBytesShareTheByteBudget() {
    TsBlock one = block(1);
    long bytes = one.getRetainedSizeInBytes();
    CteDataStore store = new CteDataStore(null, List.of(0), bytes + 100, 1);
    assertEquals(CteDataStore.AddResult.SUCCESS, store.tryAddTsBlock(one));
    assertTrue(store.tryReserveExtraBytes(100));
    assertFalse(store.tryReserveExtraBytes(1));
    store.clear();
    assertTrue(store.tryReserveExtraBytes(bytes + 100));
    assertFalse(store.tryReserveExtraBytes(1));
  }
}
