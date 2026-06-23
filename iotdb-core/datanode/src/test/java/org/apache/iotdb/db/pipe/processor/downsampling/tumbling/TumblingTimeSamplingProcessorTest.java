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

package org.apache.iotdb.db.pipe.processor.downsampling.tumbling;

import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Binary;
import org.apache.iotdb.pipe.api.type.Type;

import org.apache.tsfile.read.common.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TumblingTimeSamplingProcessorTest {

  @Test
  public void testExtremeTimestampDistanceReachesInterval() throws Exception {
    final TumblingTimeSamplingProcessor processor = new TumblingTimeSamplingProcessor();
    final TestLastObjectCache cache = new TestLastObjectCache();
    setField(processor, "intervalInCurrentPrecision", Long.MAX_VALUE);
    setField(processor, "pathLastObjectCache", cache);

    try {
      final CountingRowCollector rowCollector = new CountingRowCollector();

      processor.processRow(
          new TestRow(Long.MIN_VALUE), rowCollector, "root.db.d1", new AtomicReference<>());
      processor.processRow(
          new TestRow(Long.MAX_VALUE), rowCollector, "root.db.d1", new AtomicReference<>());
      processor.processRow(
          new TestRow(Long.MAX_VALUE - 1), rowCollector, "root.db.d1", new AtomicReference<>());

      Assert.assertEquals(2, rowCollector.getCollectedRowCount());
    } finally {
      cache.close();
    }
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class TestLastObjectCache extends PartialPathLastObjectCache<Long> {

    private TestLastObjectCache() {
      super(1024);
    }

    @Override
    protected long calculateMemoryUsage(final Long object) {
      return Long.BYTES;
    }
  }

  private static class CountingRowCollector implements RowCollector {

    private final AtomicInteger collectedRowCount = new AtomicInteger();

    @Override
    public void collectRow(final Row row) throws IOException {
      collectedRowCount.incrementAndGet();
    }

    private int getCollectedRowCount() {
      return collectedRowCount.get();
    }
  }

  private static class TestRow implements Row {

    private final long timestamp;

    private TestRow(final long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public long getTime() {
      return timestamp;
    }

    @Override
    public int getInt(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binary getBinary(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(final int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(final int columnIndex) {
      return 1;
    }

    @Override
    public Type getDataType(final int columnIndex) {
      return Type.INT32;
    }

    @Override
    public boolean isNull(final int columnIndex) {
      return false;
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public int getColumnIndex(final Path columnName) throws PipeParameterNotValidException {
      return 0;
    }

    @Override
    public String getColumnName(final int columnIndex) {
      return "s1";
    }

    @Override
    public List<Type> getColumnTypes() {
      return Collections.singletonList(Type.INT32);
    }

    @Override
    public String getDeviceId() {
      return "root.db.d1";
    }
  }
}
