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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.query;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParserMemoryBlock;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TsFileInsertionEventQueryParserTabletIteratorTest {

  @Test
  public void testDeletedRowSlotIsReusedWithCorrectNulls() throws Exception {
    final RowRecord deleted = row(0, 10L, new Binary(new byte[] {1}));
    final RowRecord sparse = row(1, 11L, null);
    final RowRecord dense = row(2, 12L, new Binary(new byte[] {2, 3}));
    final List<RowRecord> rows = Arrays.asList(deleted, sparse, dense);
    final AtomicInteger cursor = new AtomicInteger();
    final QueryDataSet dataSet = mock(QueryDataSet.class);
    when(dataSet.hasNext()).thenAnswer(invocation -> cursor.get() < rows.size());
    when(dataSet.next()).thenAnswer(invocation -> rows.get(cursor.getAndIncrement()));
    final TsFileReader reader = mock(TsFileReader.class);
    when(reader.query(any(QueryExpression.class))).thenReturn(dataSet);
    final TsFileInsertionEventParserMemoryBlock memory =
        mock(TsFileInsertionEventParserMemoryBlock.class);
    when(memory.getMemoryUsageInBytes()).thenReturn(Long.MAX_VALUE);
    final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    for (final TreeDeletionEntry deletion :
        Arrays.asList(
            new TreeDeletionEntry(new MeasurementPath("root.sg.d.s1"), 0, 0),
            new TreeDeletionEntry(new MeasurementPath("root.sg.d.s2"), 0, 0))) {
      modifications.append(deletion.keyOfPatternTree(), deletion);
    }

    final TsFileInsertionEventQueryParserTabletIterator iterator =
        new TsFileInsertionEventQueryParserTabletIterator(
            reader,
            Map.of("root.sg.d.s1", TSDataType.INT64, "root.sg.d.s2", TSDataType.STRING),
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d"),
            Arrays.asList("s2", "s1"),
            null,
            memory,
            modifications,
            new TabletStringInternPool());

    final Tablet tablet = iterator.next();
    Assert.assertEquals(2, tablet.getRowSize());
    Assert.assertEquals(1, tablet.getTimestamp(0));
    Assert.assertEquals(11L, tablet.getValue(0, 0));
    Assert.assertFalse(tablet.isNull(0, 0));
    Assert.assertTrue(tablet.isNull(0, 1));
    Assert.assertEquals(2, tablet.getTimestamp(1));
    Assert.assertEquals(12L, tablet.getValue(1, 0));
    Assert.assertEquals(dense.getFields().get(1).getBinaryV(), tablet.getValue(1, 1));
    Assert.assertFalse(tablet.isNull(1, 1));
    Assert.assertFalse(iterator.hasNext());
  }

  private static RowRecord row(final long time, final long value, final Binary binary) {
    final RowRecord row = new RowRecord(time);
    final Field number = new Field(TSDataType.INT64);
    number.setLongV(value);
    row.addField(number);
    if (binary == null) {
      row.addField(null);
    } else {
      final Field text = new Field(TSDataType.STRING);
      text.setBinaryV(binary);
      row.addField(text);
    }
    return row;
  }
}
