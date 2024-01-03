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

package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.page.LazyLoadAlignedPagePointReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LazyLoadAlignedPagePointReaderTest {

  @Test
  public void testTimeNoData() throws IOException {
    int columnCount = 2;
    TimePageReader timePageReader = PowerMockito.mock(TimePageReader.class);
    List<ValuePageReader> valuePageReaders = new LinkedList<>();
    for (int i = 0; i < columnCount; i++) {
      valuePageReaders.add(PowerMockito.mock(ValuePageReader.class));
    }

    PowerMockito.when(timePageReader.hasNextTime()).thenReturn(false);
    PowerMockito.when(valuePageReaders.get(0).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null);
    PowerMockito.when(valuePageReaders.get(1).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null);

    LazyLoadAlignedPagePointReader reader =
        new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
    boolean hasNextValue = reader.hasNextTimeValuePair();
    Assert.assertFalse(hasNextValue);
  }

  @Test
  public void testValueNoData() throws IOException {
    int columnCount = 2;
    TimePageReader timePageReader = PowerMockito.mock(TimePageReader.class);
    List<ValuePageReader> valuePageReaders = new LinkedList<>();
    for (int i = 0; i < columnCount; i++) {
      valuePageReaders.add(PowerMockito.mock(ValuePageReader.class));
    }

    PowerMockito.when(timePageReader.hasNextTime()).thenReturn(true).thenReturn(false);
    PowerMockito.when(valuePageReaders.get(0).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null);
    PowerMockito.when(valuePageReaders.get(1).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null);

    LazyLoadAlignedPagePointReader reader =
        new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
    boolean hasNextValue = reader.hasNextTimeValuePair();
    Assert.assertFalse(hasNextValue);
  }

  @Test
  public void testOneRow() throws IOException {
    int columnCount = 2;
    TimePageReader timePageReader = PowerMockito.mock(TimePageReader.class);
    List<ValuePageReader> valuePageReaders = new LinkedList<>();
    for (int i = 0; i < columnCount; i++) {
      valuePageReaders.add(PowerMockito.mock(ValuePageReader.class));
    }

    PowerMockito.when(timePageReader.hasNextTime()).thenReturn(true).thenReturn(false);
    PowerMockito.when(timePageReader.nextTime()).thenReturn(1L);
    PowerMockito.when(valuePageReaders.get(0).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(new TsPrimitiveType.TsInt(1));
    PowerMockito.when(valuePageReaders.get(1).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(new TsPrimitiveType.TsInt(2));

    LazyLoadAlignedPagePointReader reader =
        new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
    boolean hasNextValue = reader.hasNextTimeValuePair();
    Assert.assertTrue(hasNextValue);
    TimeValuePair row = reader.nextTimeValuePair();
    Assert.assertEquals(1L, row.getTimestamp());
    Assert.assertEquals(
        new TsPrimitiveType.TsVector(
            new TsPrimitiveType.TsInt[] {
              new TsPrimitiveType.TsInt(1), new TsPrimitiveType.TsInt(2)
            }),
        row.getValue());
    Assert.assertFalse(reader.hasNextTimeValuePair());
  }

  @Test
  public void testSomeColumnNull() throws IOException {
    int columnCount = 2;
    TimePageReader timePageReader = PowerMockito.mock(TimePageReader.class);
    List<ValuePageReader> valuePageReaders = new LinkedList<>();
    for (int i = 0; i < columnCount; i++) {
      valuePageReaders.add(PowerMockito.mock(ValuePageReader.class));
    }

    PowerMockito.when(timePageReader.hasNextTime())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    PowerMockito.when(timePageReader.nextTime()).thenReturn(1L).thenReturn(2L);
    PowerMockito.when(valuePageReaders.get(0).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(new TsPrimitiveType.TsInt(1))
        .thenReturn(null);
    PowerMockito.when(valuePageReaders.get(1).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null)
        .thenReturn(null);

    LazyLoadAlignedPagePointReader reader =
        new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
    boolean hasNextValue = reader.hasNextTimeValuePair();
    Assert.assertTrue(hasNextValue);
    TimeValuePair row = reader.nextTimeValuePair();
    Assert.assertEquals(1L, row.getTimestamp());
    Assert.assertEquals("[1, null]", row.getValue().toString());
    Assert.assertFalse(reader.hasNextTimeValuePair());
  }

  @Test
  public void testMultiRow() throws IOException {
    int columnCount = 2;
    TimePageReader timePageReader = PowerMockito.mock(TimePageReader.class);
    List<ValuePageReader> valuePageReaders = new LinkedList<>();
    for (int i = 0; i < columnCount; i++) {
      valuePageReaders.add(PowerMockito.mock(ValuePageReader.class));
    }

    PowerMockito.when(timePageReader.hasNextTime())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    PowerMockito.when(timePageReader.nextTime()).thenReturn(1L).thenReturn(2L);
    PowerMockito.when(valuePageReaders.get(0).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(new TsPrimitiveType.TsInt(1))
        .thenReturn(new TsPrimitiveType.TsInt(1));
    PowerMockito.when(valuePageReaders.get(1).nextValue(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(null)
        .thenReturn(new TsPrimitiveType.TsInt(2));

    LazyLoadAlignedPagePointReader reader =
        new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
    boolean hasNextValue = reader.hasNextTimeValuePair();
    Assert.assertTrue(hasNextValue);
    TimeValuePair row1 = reader.nextTimeValuePair();
    Assert.assertEquals(1L, row1.getTimestamp());
    Assert.assertEquals("[1, null]", row1.getValue().toString());
    Assert.assertTrue(reader.hasNextTimeValuePair());
    TimeValuePair row2 = reader.nextTimeValuePair();
    Assert.assertEquals(2L, row2.getTimestamp());
    Assert.assertEquals("[1, 2]", row2.getValue().toString());
    Assert.assertFalse(reader.hasNextTimeValuePair());
  }
}
