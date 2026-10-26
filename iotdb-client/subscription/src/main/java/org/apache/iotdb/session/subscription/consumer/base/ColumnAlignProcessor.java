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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A non-buffering processor that forward-fills null columns in each Tablet using the last known
 * value for the same device. This is useful for CDC scenarios where a write only updates a subset
 * of columns, leaving others null; the processor fills them with the most recent value.
 *
 * <p>State is maintained per topic and per device. Tree-model tablets use {@code
 * Tablet.getDeviceId()}; table-model tablets use {@code Tablet.getDeviceID(row)} because each row
 * may belong to a different table device even when the table has no TAG column.
 */
public class ColumnAlignProcessor implements SubscriptionMessageProcessor {

  public enum Dialect {
    TREE,
    TABLE
  }

  // topicName -> deviceKey -> columnName -> latest non-null value with its timestamp
  private final Map<String, Map<Object, Map<String, Pair<Long, Object>>>> lastValues =
      new HashMap<>();

  private final Dialect dialect;

  public ColumnAlignProcessor() {
    this(Dialect.TREE);
  }

  public ColumnAlignProcessor(final Dialect dialect) {
    this.dialect = Objects.requireNonNull(dialect, SubscriptionMessages.DIALECT_NOT_NULL);
  }

  @Override
  public List<SubscriptionMessage> process(final List<SubscriptionMessage> messages) {
    for (final SubscriptionMessage message : messages) {
      if (message.getMessageType() != SubscriptionMessageType.RECORD_HANDLER.getType()) {
        continue;
      }
      final String topicName = message.getCommitContext().getTopicName();
      final Iterator<Tablet> tablets = message.getRecordTabletIterator();
      while (tablets.hasNext()) {
        fillTablet(topicName, tablets.next());
      }
    }
    return messages;
  }

  @Override
  public List<SubscriptionMessage> flush() {
    return Collections.emptyList();
  }

  @Override
  public void reset() {
    lastValues.clear();
  }

  @Override
  public boolean supportsTopicScopedReset() {
    return true;
  }

  @Override
  public void reset(final String topicName) {
    lastValues.remove(topicName);
  }

  private void fillTablet(final String topicName, final Tablet tablet) {
    final Map<Object, Map<String, Pair<Long, Object>>> topicCache =
        lastValues.computeIfAbsent(topicName, ignored -> new HashMap<>());

    final Object[] values = tablet.getValues();
    final BitMap[] bitMaps = tablet.getBitMaps();
    final int rowSize = tablet.getRowSize();
    final int columnCount = values.length;

    for (int row = 0; row < rowSize; row++) {
      final long timestamp = tablet.getTimestamp(row);
      final Object deviceKey = getDeviceKey(tablet, row);
      if (deviceKey == null) {
        continue;
      }
      final Map<String, Pair<Long, Object>> cache =
          topicCache.computeIfAbsent(deviceKey, ignored -> new HashMap<>());
      for (int col = 0; col < columnCount; col++) {
        final String columnKey = getColumnKey(tablet, col);
        final boolean isNull =
            bitMaps != null && bitMaps[col] != null && bitMaps[col].isMarked(row);
        if (isNull) {
          final Pair<Long, Object> cached = cache.get(columnKey);
          if (cached != null && cached.left < timestamp) {
            setValueAt(values[col], row, cached.right);
            bitMaps[col].unmark(row);
          }
        } else {
          final Pair<Long, Object> cached = cache.get(columnKey);
          if (cached == null) {
            cache.put(columnKey, new Pair<>(timestamp, getValueAt(values[col], row)));
          } else if (timestamp >= cached.left) {
            cached.left = timestamp;
            cached.right = getValueAt(values[col], row);
          }
        }
      }
    }
  }

  private Object getDeviceKey(final Tablet tablet, final int row) {
    if (dialect == Dialect.TABLE) {
      final IDeviceID deviceID = tablet.getDeviceID(row);
      return Objects.nonNull(deviceID) ? deviceID : tablet.getTableName();
    }
    return tablet.getDeviceId();
  }

  private static String getColumnKey(final Tablet tablet, final int columnIndex) {
    return tablet.getSchemas().get(columnIndex).getMeasurementName();
  }

  private static Object getValueAt(final Object columnArray, final int row) {
    if (columnArray instanceof long[]) {
      return ((long[]) columnArray)[row];
    } else if (columnArray instanceof int[]) {
      return ((int[]) columnArray)[row];
    } else if (columnArray instanceof double[]) {
      return ((double[]) columnArray)[row];
    } else if (columnArray instanceof float[]) {
      return ((float[]) columnArray)[row];
    } else if (columnArray instanceof boolean[]) {
      return ((boolean[]) columnArray)[row];
    } else if (columnArray instanceof Object[]) {
      return ((Object[]) columnArray)[row];
    }
    return null;
  }

  private static void setValueAt(final Object columnArray, final int row, final Object value) {
    if (columnArray instanceof long[]) {
      ((long[]) columnArray)[row] = (Long) value;
    } else if (columnArray instanceof int[]) {
      ((int[]) columnArray)[row] = (Integer) value;
    } else if (columnArray instanceof double[]) {
      ((double[]) columnArray)[row] = (Double) value;
    } else if (columnArray instanceof float[]) {
      ((float[]) columnArray)[row] = (Float) value;
    } else if (columnArray instanceof boolean[]) {
      ((boolean[]) columnArray)[row] = (Boolean) value;
    } else if (columnArray instanceof Object[]) {
      ((Object[]) columnArray)[row] = value;
    }
  }
}
