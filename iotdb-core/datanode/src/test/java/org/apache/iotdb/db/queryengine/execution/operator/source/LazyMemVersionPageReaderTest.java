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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryStatistics;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil.LazyMemVersionPageReader;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;

import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class LazyMemVersionPageReaderTest {

  private final boolean aligned;
  private QueryStatistics statistics;
  private MemPointIterator iterator;
  private LazyMemVersionPageReader reader;

  @Parameterized.Parameters(name = "aligned={0}")
  public static Object[][] parameters() {
    return new Object[][] {{false}, {true}};
  }

  public LazyMemVersionPageReaderTest(boolean aligned) {
    this.aligned = aligned;
  }

  @Before
  public void setUp() {
    QueryContext context = new QueryContext(false, false);
    statistics = context.getQueryStatistics();
    iterator = mock(MemPointIterator.class);
    reader =
        new LazyMemVersionPageReader(
            context, 0, 0, 0, aligned, new IntegerStatistics(), iterator, true);
  }

  @Test
  public void testNextBatchAccumulatesDecodeStatistics() {
    TsBlock batch = new TsBlock(0);
    when(iterator.nextBatch()).thenReturn(batch);

    assertSame(batch, reader.nextBatch());
    assertDecodeStatistics(1);
    assertSame(batch, reader.nextBatch());
    assertDecodeStatistics(2);
    verify(iterator, times(2)).nextBatch();
  }

  @Test
  public void testNextBatchRecordsDecodeStatisticsOnFailure() {
    RuntimeException failure = new RuntimeException();
    when(iterator.nextBatch()).thenThrow(failure);

    assertSame(failure, assertThrows(RuntimeException.class, reader::nextBatch));
    assertDecodeStatistics(1);
    verify(iterator).nextBatch();
  }

  private void assertDecodeStatistics(long count) {
    assertEquals(aligned ? count : 0, statistics.getPageReadersDecodeAlignedMemCount().get());
    assertEquals(aligned ? 0 : count, statistics.getPageReadersDecodeNonAlignedMemCount().get());
    if (aligned) {
      assertTrue(statistics.getPageReadersDecodeAlignedMemTime().get() > 0);
      assertEquals(0, statistics.getPageReadersDecodeNonAlignedMemTime().get());
    } else {
      assertEquals(0, statistics.getPageReadersDecodeAlignedMemTime().get());
      assertTrue(statistics.getPageReadersDecodeNonAlignedMemTime().get() > 0);
    }
    assertEquals(0, statistics.getPageReadersDecodeAlignedDiskCount().get());
    assertEquals(0, statistics.getPageReadersDecodeNonAlignedDiskCount().get());
    assertEquals(0, statistics.getPageReadersDecodeAlignedDiskTime().get());
    assertEquals(0, statistics.getPageReadersDecodeNonAlignedDiskTime().get());
  }
}
