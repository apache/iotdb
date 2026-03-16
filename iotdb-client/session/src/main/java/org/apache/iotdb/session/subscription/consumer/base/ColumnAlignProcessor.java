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

import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSetsHandler;

import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A non-buffering processor that forward-fills null columns in each Tablet using the last known
 * value for the same device/table. This is useful for CDC scenarios where a write only updates a
 * subset of columns, leaving others null; the processor fills them with the most recent value.
 *
 * <p>State is maintained per device (identified by {@code Tablet.getDeviceId()} for tree-model or
 * {@code Tablet.getTableName()} for table-model).
 */
public class ColumnAlignProcessor implements SubscriptionMessageProcessor {

  // deviceKey -> (columnIndex -> lastValue)
  private final Map<String, Map<Integer, Object>> lastValues = new HashMap<>();

  @Override
  public List<SubscriptionMessage> process(final List<SubscriptionMessage> messages) {
    for (final SubscriptionMessage message : messages) {
      if (message.getMessageType() != SubscriptionMessageType.SESSION_DATA_SETS_HANDLER.getType()) {
        continue;
      }
      final SubscriptionSessionDataSetsHandler handler = message.getSessionDataSetsHandler();
      for (final SubscriptionSessionDataSet dataSet : handler) {
        fillTablet(dataSet.getTablet());
      }
    }
    return messages;
  }

  @Override
  public List<SubscriptionMessage> flush() {
    return Collections.emptyList();
  }

  private void fillTablet(final Tablet tablet) {
    final String deviceKey = getDeviceKey(tablet);
    final Map<Integer, Object> cache = lastValues.computeIfAbsent(deviceKey, k -> new HashMap<>());

    final Object[] values = tablet.getValues();
    final BitMap[] bitMaps = tablet.getBitMaps();
    final int rowSize = tablet.getRowSize();
    final int columnCount = values.length;

    for (int row = 0; row < rowSize; row++) {
      for (int col = 0; col < columnCount; col++) {
        final boolean isNull =
            bitMaps != null && bitMaps[col] != null && bitMaps[col].isMarked(row);
        if (isNull) {
          // try forward-fill from cache
          final Object cached = cache.get(col);
          if (cached != null) {
            setValueAt(values[col], row, cached);
            bitMaps[col].unmark(row);
          }
        } else {
          // update cache with this non-null value
          cache.put(col, getValueAt(values[col], row));
        }
      }
    }
  }

  private static String getDeviceKey(final Tablet tablet) {
    // tree model uses deviceId; table model uses tableName
    final String deviceId = tablet.getDeviceId();
    return deviceId != null ? deviceId : tablet.getTableName();
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
