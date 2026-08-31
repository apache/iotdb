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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.utils.BitMapUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PipeTabletUtils {

  private PipeTabletUtils() {}

  public static final class TabletStringInternPool {

    private final Map<String, String> internedStrings = new HashMap<>();

    public String intern(final String value) {
      if (Objects.isNull(value)) {
        return null;
      }

      final String internedValue = internedStrings.get(value);
      if (Objects.nonNull(internedValue)) {
        return internedValue;
      }

      internedStrings.put(value, value);
      return value;
    }

    public void intern(final String[] values) {
      if (Objects.isNull(values)) {
        return;
      }

      for (int i = 0; i < values.length; ++i) {
        values[i] = intern(values[i]);
      }
    }

    public void intern(final List<String> values) {
      if (Objects.isNull(values)) {
        return;
      }

      for (int i = 0; i < values.size(); ++i) {
        values.set(i, intern(values.get(i)));
      }
    }

    public Tablet intern(final Tablet tablet) {
      if (Objects.isNull(tablet)) {
        return null;
      }

      tablet.setInsertTargetName(intern(tablet.getDeviceId()));
      internMeasurementSchemas(tablet.getSchemas());
      return tablet;
    }

    public void internMeasurementSchemas(final List<IMeasurementSchema> schemas) {
      if (Objects.isNull(schemas)) {
        return;
      }

      for (final IMeasurementSchema schema : schemas) {
        if (schema instanceof MeasurementSchema) {
          intern((MeasurementSchema) schema);
        }
      }
    }

    public MeasurementSchema intern(final MeasurementSchema schema) {
      if (Objects.isNull(schema)) {
        return null;
      }

      schema.setMeasurementName(intern(schema.getMeasurementName()));
      schema.setProps(intern(schema.getProps()));
      return schema;
    }

    private Map<String, String> intern(final Map<String, String> props) {
      if (Objects.isNull(props) || props.isEmpty()) {
        return props;
      }

      final Map<String, String> internedProps = new HashMap<>(props.size());
      for (final Map.Entry<String, String> entry : props.entrySet()) {
        internedProps.put(intern(entry.getKey()), intern(entry.getValue()));
      }
      return internedProps;
    }
  }

  public static Tablet internTablet(
      final Tablet tablet, final TabletStringInternPool tabletStringInternPool) {
    return Objects.nonNull(tabletStringInternPool) ? tabletStringInternPool.intern(tablet) : tablet;
  }

  public static void compactBitMaps(final Tablet tablet) {
    if (Objects.isNull(tablet)) {
      return;
    }
    tablet.setBitMaps(compactBitMaps(tablet.getBitMaps(), tablet.getRowSize()));
  }

  public static BitMap[] compactBitMaps(final BitMap[] bitMaps, final int rowCount) {
    return BitMapUtils.compactBitMaps(bitMaps, rowCount);
  }

  public static BitMap[] copyBitMapsOrCreateEmpty(final Tablet tablet) {
    final BitMap[] bitMaps = tablet.getBitMaps();
    return Objects.nonNull(bitMaps)
        ? Arrays.copyOf(bitMaps, bitMaps.length)
        : new BitMap[getColumnCount(tablet)];
  }

  public static void markNullValue(final Tablet tablet, final int rowIndex, final int columnIndex) {
    final BitMap[] bitMaps = ensureBitMaps(tablet, columnIndex + 1);
    if (Objects.isNull(bitMaps[columnIndex])) {
      bitMaps[columnIndex] = new BitMap(tablet.getMaxRowNumber());
    }
    bitMaps[columnIndex].mark(rowIndex);
  }

  public static void putTimestamp(final Tablet tablet, final int rowIndex, final long timestamp) {
    tablet.getTimestamps()[rowIndex] = timestamp;
    tablet.setRowSize(Math.max(tablet.getRowSize(), rowIndex + 1));
  }

  public static void putValue(
      final Tablet tablet,
      final int rowIndex,
      final int columnIndex,
      final TSDataType dataType,
      final Object value) {
    switch (dataType) {
      case BOOLEAN:
        ((boolean[]) tablet.getValues()[columnIndex])[rowIndex] = (Boolean) value;
        break;
      case INT32:
        ((int[]) tablet.getValues()[columnIndex])[rowIndex] = (Integer) value;
        break;
      case DATE:
        ((LocalDate[]) tablet.getValues()[columnIndex])[rowIndex] = (LocalDate) value;
        break;
      case INT64:
      case TIMESTAMP:
        ((long[]) tablet.getValues()[columnIndex])[rowIndex] = (Long) value;
        break;
      case FLOAT:
        ((float[]) tablet.getValues()[columnIndex])[rowIndex] = (Float) value;
        break;
      case DOUBLE:
        ((double[]) tablet.getValues()[columnIndex])[rowIndex] = (Double) value;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        ((Binary[]) tablet.getValues()[columnIndex])[rowIndex] = toBinary(value);
        break;
      default:
        throw new UnSupportedDataTypeException(DataNodePipeMessages.UNSUPPORTED + dataType);
    }
    unmarkNullValue(tablet, rowIndex, columnIndex);
  }

  private static void unmarkNullValue(
      final Tablet tablet, final int rowIndex, final int columnIndex) {
    final BitMap[] bitMaps = tablet.getBitMaps();
    if (Objects.nonNull(bitMaps)
        && columnIndex < bitMaps.length
        && Objects.nonNull(bitMaps[columnIndex])) {
      bitMaps[columnIndex].unmark(rowIndex);
    }
  }

  private static BitMap[] ensureBitMaps(final Tablet tablet, final int minColumnCount) {
    final int columnCount = Math.max(getColumnCount(tablet), minColumnCount);
    BitMap[] bitMaps = tablet.getBitMaps();
    if (Objects.isNull(bitMaps)) {
      bitMaps = new BitMap[columnCount];
      tablet.setBitMaps(bitMaps);
    } else if (bitMaps.length < columnCount) {
      final BitMap[] expandedBitMaps = new BitMap[columnCount];
      System.arraycopy(bitMaps, 0, expandedBitMaps, 0, bitMaps.length);
      bitMaps = expandedBitMaps;
      tablet.setBitMaps(bitMaps);
    }
    return bitMaps;
  }

  private static int getColumnCount(final Tablet tablet) {
    if (Objects.nonNull(tablet.getSchemas())) {
      return tablet.getSchemas().size();
    }
    return Objects.nonNull(tablet.getValues()) ? tablet.getValues().length : 0;
  }

  private static Binary toBinary(final Object value) {
    if (Objects.isNull(value)) {
      return Binary.EMPTY_VALUE;
    }
    if (value instanceof Binary) {
      return (Binary) value;
    }
    if (value instanceof byte[]) {
      return new Binary((byte[]) value);
    }
    if (value instanceof String) {
      return new Binary(((String) value).getBytes(TSFileConfig.STRING_CHARSET));
    }
    throw new IllegalArgumentException(
        String.format(
            DataNodePipeMessages.PIPE_EXCEPTION_EXPECTED_BINARY_BYTE_OR_STRING_BUT_WAS_S_7976B10F,
            value.getClass()));
  }
}
