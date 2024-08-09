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

package org.apache.iotdb.db.pipe.receiver.transform.converter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

public class ArrayConverter {

  @FunctionalInterface
  private interface Converter {
    Object convert(
        final TSDataType sourceDataType,
        final TSDataType targetDataType,
        final Object sourceValues);
  }

  private static final Converter[][] CONVERTER =
      new Converter[TSDataType.values().length][TSDataType.values().length];

  private static final Converter DO_NOTHING_CONVERTER =
      (sourceDataType, targetDataType, sourceValues) -> sourceValues;

  static {
    for (final TSDataType sourceDataType : TSDataType.values()) {
      for (final TSDataType targetDataType : TSDataType.values()) {
        CONVERTER[sourceDataType.ordinal()][targetDataType.ordinal()] = DO_NOTHING_CONVERTER;
      }
    }

    // BOOLEAN
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final int[] intValues = new int[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            intValues[i] = ValueConverter.convertBooleanToInt32(boolValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final long[] longValues = new long[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            longValues[i] = ValueConverter.convertBooleanToInt64(boolValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final float[] floatValues = new float[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            floatValues[i] = ValueConverter.convertBooleanToFloat(boolValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final double[] doubleValues = new double[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            doubleValues[i] = ValueConverter.convertBooleanToDouble(boolValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final Binary[] textValues = new Binary[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            textValues[i] = ValueConverter.convertBooleanToText(boolValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final long[] timestampValues = new long[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            timestampValues[i] = ValueConverter.convertBooleanToTimestamp(boolValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final int[] dateValues = new int[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            dateValues[i] = ValueConverter.convertBooleanToDate(boolValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final Binary[] blobValues = new Binary[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            blobValues[i] = ValueConverter.convertBooleanToBlob(boolValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final boolean[] boolValues = (boolean[]) sourceValues;
          final Binary[] stringValues = new Binary[boolValues.length];
          for (int i = 0; i < boolValues.length; i++) {
            stringValues[i] = ValueConverter.convertBooleanToString(boolValues[i]);
          }
          return stringValues;
        };

    // INT32
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final boolean[] boolValues = new boolean[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            boolValues[i] = ValueConverter.convertInt32ToBoolean(intValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final long[] longValues = new long[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            longValues[i] = ValueConverter.convertInt32ToInt64(intValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final float[] floatValues = new float[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            floatValues[i] = ValueConverter.convertInt32ToFloat(intValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final double[] doubleValues = new double[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            doubleValues[i] = ValueConverter.convertInt32ToDouble(intValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final Binary[] textValues = new Binary[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            textValues[i] = ValueConverter.convertInt32ToText(intValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final long[] timestampValues = new long[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            timestampValues[i] = ValueConverter.convertInt32ToTimestamp(intValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final int[] dateValues = new int[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            dateValues[i] = ValueConverter.convertInt32ToDate(intValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final Binary[] blobValues = new Binary[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            blobValues[i] = ValueConverter.convertInt32ToBlob(intValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] intValues = (int[]) sourceValues;
          final Binary[] stringValues = new Binary[intValues.length];
          for (int i = 0; i < intValues.length; i++) {
            stringValues[i] = ValueConverter.convertInt32ToString(intValues[i]);
          }
          return stringValues;
        };

    // INT64
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final boolean[] boolValues = new boolean[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            boolValues[i] = ValueConverter.convertInt64ToBoolean(longValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final int[] intValues = new int[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            intValues[i] = ValueConverter.convertInt64ToInt32(longValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final float[] floatValues = new float[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            floatValues[i] = ValueConverter.convertInt64ToFloat(longValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final double[] doubleValues = new double[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            doubleValues[i] = ValueConverter.convertInt64ToDouble(longValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final Binary[] textValues = new Binary[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            textValues[i] = ValueConverter.convertInt64ToText(longValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final long[] timestampValues = new long[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            timestampValues[i] = ValueConverter.convertInt64ToTimestamp(longValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final int[] dateValues = new int[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            dateValues[i] = ValueConverter.convertInt64ToDate(longValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final Binary[] blobValues = new Binary[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            blobValues[i] = ValueConverter.convertInt64ToBlob(longValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] longValues = (long[]) sourceValues;
          final Binary[] stringValues = new Binary[longValues.length];
          for (int i = 0; i < longValues.length; i++) {
            stringValues[i] = ValueConverter.convertInt64ToString(longValues[i]);
          }
          return stringValues;
        };

    // FLOAT
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final boolean[] boolValues = new boolean[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            boolValues[i] = ValueConverter.convertFloatToBoolean(floatValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final int[] intValues = new int[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            intValues[i] = ValueConverter.convertFloatToInt32(floatValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final long[] longValues = new long[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            longValues[i] = ValueConverter.convertFloatToInt64(floatValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final double[] doubleValues = new double[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            doubleValues[i] = ValueConverter.convertFloatToDouble(floatValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final Binary[] textValues = new Binary[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            textValues[i] = ValueConverter.convertFloatToText(floatValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final long[] timestampValues = new long[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            timestampValues[i] = ValueConverter.convertFloatToTimestamp(floatValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final int[] dateValues = new int[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            dateValues[i] = ValueConverter.convertFloatToDate(floatValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final Binary[] blobValues = new Binary[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            blobValues[i] = ValueConverter.convertFloatToBlob(floatValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final float[] floatValues = (float[]) sourceValues;
          final Binary[] stringValues = new Binary[floatValues.length];
          for (int i = 0; i < floatValues.length; i++) {
            stringValues[i] = ValueConverter.convertFloatToString(floatValues[i]);
          }
          return stringValues;
        };

    // DOUBLE
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final boolean[] boolValues = new boolean[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            boolValues[i] = ValueConverter.convertDoubleToBoolean(doubleValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final int[] intValues = new int[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            intValues[i] = ValueConverter.convertDoubleToInt32(doubleValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final long[] longValues = new long[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            longValues[i] = ValueConverter.convertDoubleToInt64(doubleValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final float[] floatValues = new float[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            floatValues[i] = ValueConverter.convertDoubleToFloat(doubleValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final Binary[] textValues = new Binary[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            textValues[i] = ValueConverter.convertDoubleToText(doubleValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final long[] timestampValues = new long[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            timestampValues[i] = ValueConverter.convertDoubleToTimestamp(doubleValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final int[] dateValues = new int[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            dateValues[i] = ValueConverter.convertDoubleToDate(doubleValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final Binary[] blobValues = new Binary[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            blobValues[i] = ValueConverter.convertDoubleToBlob(doubleValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final double[] doubleValues = (double[]) sourceValues;
          final Binary[] stringValues = new Binary[doubleValues.length];
          for (int i = 0; i < doubleValues.length; i++) {
            stringValues[i] = ValueConverter.convertDoubleToString(doubleValues[i]);
          }
          return stringValues;
        };

    // TEXT
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final boolean[] boolValues = new boolean[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            boolValues[i] = ValueConverter.convertTextToBoolean(textValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final int[] intValues = new int[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            intValues[i] = ValueConverter.convertTextToInt32(textValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final long[] longValues = new long[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            longValues[i] = ValueConverter.convertTextToInt64(textValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final float[] floatValues = new float[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            floatValues[i] = ValueConverter.convertTextToFloat(textValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final double[] doubleValues = new double[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            doubleValues[i] = ValueConverter.convertTextToDouble(textValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final long[] timestampValues = new long[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            timestampValues[i] = ValueConverter.convertTextToTimestamp(textValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final int[] dateValues = new int[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            dateValues[i] = ValueConverter.convertTextToDate(textValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final Binary[] blobValues = new Binary[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            blobValues[i] = ValueConverter.convertTextToBlob(textValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] textValues = (Binary[]) sourceValues;
          final Binary[] stringValues = new Binary[textValues.length];
          for (int i = 0; i < textValues.length; i++) {
            stringValues[i] = ValueConverter.convertTextToString(textValues[i]);
          }
          return stringValues;
        };

    // VECTOR
    for (int i = 0; i < TSDataType.values().length; i++) {
      CONVERTER[TSDataType.VECTOR.ordinal()][i] = DO_NOTHING_CONVERTER;
    }

    // UNKNOWN
    for (int i = 0; i < TSDataType.values().length; i++) {
      CONVERTER[TSDataType.UNKNOWN.ordinal()][i] = DO_NOTHING_CONVERTER;
    }

    // TIMESTAMP
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final boolean[] boolValues = new boolean[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            boolValues[i] = ValueConverter.convertTimestampToBoolean(timestampValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final int[] intValues = new int[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            intValues[i] = ValueConverter.convertTimestampToInt32(timestampValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final long[] longValues = new long[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            longValues[i] = ValueConverter.convertTimestampToInt64(timestampValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final float[] floatValues = new float[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            floatValues[i] = ValueConverter.convertTimestampToFloat(timestampValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final double[] doubleValues = new double[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            doubleValues[i] = ValueConverter.convertTimestampToDouble(timestampValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final Binary[] textValues = new Binary[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            textValues[i] = ValueConverter.convertTimestampToText(timestampValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final int[] dateValues = new int[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            dateValues[i] = ValueConverter.convertTimestampToDate(timestampValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final Binary[] blobValues = new Binary[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            blobValues[i] = ValueConverter.convertTimestampToBlob(timestampValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final long[] timestampValues = (long[]) sourceValues;
          final Binary[] stringValues = new Binary[timestampValues.length];
          for (int i = 0; i < timestampValues.length; i++) {
            stringValues[i] = ValueConverter.convertTimestampToString(timestampValues[i]);
          }
          return stringValues;
        };

    // DATE
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final boolean[] boolValues = new boolean[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            boolValues[i] = ValueConverter.convertDateToBoolean(dateValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final int[] intValues = new int[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            intValues[i] = ValueConverter.convertDateToInt32(dateValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final long[] longValues = new long[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            longValues[i] = ValueConverter.convertDateToInt64(dateValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final float[] floatValues = new float[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            floatValues[i] = ValueConverter.convertDateToFloat(dateValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final double[] doubleValues = new double[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            doubleValues[i] = ValueConverter.convertDateToDouble(dateValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final Binary[] textValues = new Binary[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            textValues[i] = ValueConverter.convertDateToText(dateValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final long[] timestampValues = new long[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            timestampValues[i] = ValueConverter.convertDateToTimestamp(dateValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final Binary[] blobValues = new Binary[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            blobValues[i] = ValueConverter.convertDateToBlob(dateValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final int[] dateValues = (int[]) sourceValues;
          final Binary[] stringValues = new Binary[dateValues.length];
          for (int i = 0; i < dateValues.length; i++) {
            stringValues[i] = ValueConverter.convertDateToString(dateValues[i]);
          }
          return stringValues;
        };

    // BLOB
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final boolean[] boolValues = new boolean[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            boolValues[i] = ValueConverter.convertBlobToBoolean(blobValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final int[] intValues = new int[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            intValues[i] = ValueConverter.convertBlobToInt32(blobValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final long[] longValues = new long[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            longValues[i] = ValueConverter.convertBlobToInt64(blobValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final float[] floatValues = new float[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            floatValues[i] = ValueConverter.convertBlobToFloat(blobValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final double[] doubleValues = new double[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            doubleValues[i] = ValueConverter.convertBlobToDouble(blobValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final Binary[] textValues = new Binary[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            textValues[i] = ValueConverter.convertBlobToText(blobValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final long[] timestampValues = new long[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            timestampValues[i] = ValueConverter.convertBlobToTimestamp(blobValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final int[] dateValues = new int[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            dateValues[i] = ValueConverter.convertBlobToDate(blobValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] blobValues = (Binary[]) sourceValues;
          final Binary[] stringValues = new Binary[blobValues.length];
          for (int i = 0; i < blobValues.length; i++) {
            stringValues[i] = ValueConverter.convertBlobToString(blobValues[i]);
          }
          return stringValues;
        };

    // STRING
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final boolean[] boolValues = new boolean[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            boolValues[i] = ValueConverter.convertStringToBoolean(stringValues[i]);
          }
          return boolValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final int[] intValues = new int[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            intValues[i] = ValueConverter.convertStringToInt32(stringValues[i]);
          }
          return intValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final long[] longValues = new long[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            longValues[i] = ValueConverter.convertStringToInt64(stringValues[i]);
          }
          return longValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final float[] floatValues = new float[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            floatValues[i] = ValueConverter.convertStringToFloat(stringValues[i]);
          }
          return floatValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final double[] doubleValues = new double[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            doubleValues[i] = ValueConverter.convertStringToDouble(stringValues[i]);
          }
          return doubleValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final Binary[] textValues = new Binary[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            textValues[i] = ValueConverter.convertStringToText(stringValues[i]);
          }
          return textValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.VECTOR.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.UNKNOWN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final long[] timestampValues = new long[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            timestampValues[i] = ValueConverter.convertStringToTimestamp(stringValues[i]);
          }
          return timestampValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final int[] dateValues = new int[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            dateValues[i] = ValueConverter.convertStringToDate(stringValues[i]);
          }
          return dateValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> {
          final Binary[] stringValues = (Binary[]) sourceValues;
          final Binary[] blobValues = new Binary[stringValues.length];
          for (int i = 0; i < stringValues.length; i++) {
            blobValues[i] = ValueConverter.convertStringToBlob(stringValues[i]);
          }
          return blobValues;
        };
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValues) -> sourceValues;
  }

  public static Object convert(
      final TSDataType sourceDataType, final TSDataType targetDataType, final Object sourceValues) {
    return sourceValues == null
        ? null
        : CONVERTER[sourceDataType.ordinal()][targetDataType.ordinal()].convert(
            sourceDataType, targetDataType, sourceValues);
  }

  private ArrayConverter() {
    // forbidden to construct
  }
}
