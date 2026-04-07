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

package org.apache.iotdb.subscription.it;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public final class SubscriptionTreeReaderTestUtils {

  private SubscriptionTreeReaderTestUtils() {
    // utility class
  }

  public static QueryDataSetAdapter query(final ITsFileTreeReader reader, final Path path)
      throws IOException {
    return query(reader, Collections.singletonList(path));
  }

  public static QueryDataSetAdapter query(final ITsFileTreeReader reader, final List<Path> paths)
      throws IOException {
    final Set<String> devices = new LinkedHashSet<>();
    final Set<String> measurements = new LinkedHashSet<>();
    for (final Path path : paths) {
      devices.add(path.getDeviceString());
      measurements.add(path.getMeasurement());
    }

    final List<String> deviceList = new ArrayList<>(devices);
    final List<String> measurementList = new ArrayList<>(measurements);
    final List<Integer> selectedColumnIndexes = new ArrayList<>(paths.size());
    for (final Path path : paths) {
      final int deviceIndex = deviceList.indexOf(path.getDeviceString());
      final int measurementIndex = measurementList.indexOf(path.getMeasurement());
      selectedColumnIndexes.add(2 + deviceIndex * measurementList.size() + measurementIndex);
    }

    return new QueryDataSetAdapter(
        reader.query(deviceList, measurementList, Long.MIN_VALUE, Long.MAX_VALUE),
        Collections.unmodifiableList(new ArrayList<>(paths)),
        selectedColumnIndexes);
  }

  public static final class QueryDataSetAdapter implements AutoCloseable {

    private final ResultSet resultSet;

    private final List<Path> paths;

    private final List<Integer> selectedColumnIndexes;

    private RowRecord nextRowRecord;

    private boolean initialized;

    private boolean hasNext;

    private QueryDataSetAdapter(
        final ResultSet resultSet,
        final List<Path> paths,
        final List<Integer> selectedColumnIndexes) {
      this.resultSet = resultSet;
      this.paths = paths;
      this.selectedColumnIndexes = selectedColumnIndexes;
    }

    public boolean hasNext() throws IOException {
      if (!initialized) {
        advance();
      }
      return hasNext;
    }

    public RowRecord next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final RowRecord current = nextRowRecord;
      advance();
      return current;
    }

    public List<Path> getPaths() {
      return paths;
    }

    @Override
    public void close() {
      resultSet.close();
    }

    private void advance() throws IOException {
      initialized = true;
      hasNext = resultSet.next();
      if (!hasNext) {
        nextRowRecord = null;
        return;
      }

      nextRowRecord = new RowRecord(resultSet.getLong(1), selectedColumnIndexes.size());
      for (int i = 0; i < selectedColumnIndexes.size(); ++i) {
        final int columnIndex = selectedColumnIndexes.get(i);
        final TSDataType dataType = resultSet.getMetadata().getColumnType(columnIndex);
        nextRowRecord.setField(
            i,
            resultSet.isNull(columnIndex) ? null : getValue(resultSet, columnIndex, dataType),
            dataType);
      }
    }
  }

  private static Object getValue(
      final ResultSet resultSet, final int columnIndex, final TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return resultSet.getBoolean(columnIndex);
      case INT32:
        return resultSet.getInt(columnIndex);
      case INT64:
      case TIMESTAMP:
        return resultSet.getLong(columnIndex);
      case FLOAT:
        return resultSet.getFloat(columnIndex);
      case DOUBLE:
        return resultSet.getDouble(columnIndex);
      case DATE:
        return resultSet.getDate(columnIndex);
      case TEXT:
      case BLOB:
      case STRING:
        return new Binary(resultSet.getBinary(columnIndex));
      default:
        return resultSet.getString(columnIndex);
    }
  }
}
