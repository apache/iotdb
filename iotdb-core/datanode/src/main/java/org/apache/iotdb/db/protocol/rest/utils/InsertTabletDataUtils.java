/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.utils;

import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class InsertTabletDataUtils {
  public static boolean checkSorted(List<Long> times) {
    for (int i = 1; i < times.size(); i++) {
      if (times.get(i) < times.get(i - 1)) {
        return false;
      }
    }
    return true;
  }

  public static int[] sortTimeStampList(List<Long> list) {
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < list.size(); i++) {
      indexes.add(i);
    }
    indexes.sort(Comparator.comparing(list::get));
    return indexes.stream().mapToInt(Integer::intValue).toArray();
  }

  public static <T> List<List<T>> sortList(List<List<T>> values, int[] index, int num) {
    List<List<T>> sortedValues = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      sortedValues.add(sortValueList(values.get(i), index));
    }
    return sortedValues;
  }

  /**
   * Sort the input source list.
   *
   * <p>e.g. source:[1,0,3,2,4], index: [1,2,3,4,5], return : [2,1,4,3,5]
   *
   * @param source Input list
   * @param index return order
   * @param <T> Input type
   * @return ordered list
   */
  private static <T> List<T> sortValueList(List<T> source, int[] index) {
    return Arrays.stream(index).mapToObj(source::get).collect(Collectors.toList());
  }

  public static Object[] genTabletValue(
      String device,
      String[] measurements,
      long[] times,
      TSDataType[] dataTypes,
      List<List<Object>> rawData,
      BitMap[] bitMaps)
      throws DataTypeMismatchException {
    int rowSize = times.length;
    int columnSize = dataTypes.length;
    Object[] columns = new Object[columnSize];
    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      bitMaps[columnIndex] = new BitMap(rowSize);
      switch (dataTypes[columnIndex]) {
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Boolean) {
              booleanValues[rowIndex] = (Boolean) object;
            } else {
              if ("1".equals(object.toString())) {
                booleanValues[rowIndex] = true;
              } else if ("0".equals(object.toString())) {
                booleanValues[rowIndex] = false;
              } else {
                throw new DataTypeMismatchException(
                    device,
                    measurements[columnIndex],
                    dataTypes[columnIndex],
                    times[rowIndex],
                    object);
              }
            }
          }
          columns[columnIndex] = booleanValues;
          break;
        case INT32:
        case DATE:
          int[] intValues = new int[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer) {
              intValues[rowIndex] = (Integer) object;
            } else {
              throw new DataTypeMismatchException(
                  device,
                  measurements[columnIndex],
                  dataTypes[columnIndex],
                  times[rowIndex],
                  object);
            }
          }
          columns[columnIndex] = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = new long[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object object = rawData.get(columnIndex).get(rowIndex);
            if (object == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else if (object instanceof Integer || object instanceof Long) {
              longValues[rowIndex] = ((Number) object).longValue();
            } else {
              throw new DataTypeMismatchException(
                  device,
                  measurements[columnIndex],
                  dataTypes[columnIndex],
                  times[rowIndex],
                  object);
            }
          }
          columns[columnIndex] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            Object data = rawData.get(columnIndex).get(rowIndex);
            if (data == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              floatValues[rowIndex] = Float.parseFloat(String.valueOf(data));
            }
          }
          columns[columnIndex] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
            } else {
              doubleValues[rowIndex] =
                  Double.parseDouble(String.valueOf(rawData.get(columnIndex).get(rowIndex)));
            }
          }
          columns[columnIndex] = doubleValues;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary[] binaryValues = new Binary[rowSize];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            if (rawData.get(columnIndex).get(rowIndex) == null) {
              bitMaps[columnIndex].mark(rowIndex);
              binaryValues[rowIndex] = new Binary("".getBytes(StandardCharsets.UTF_8));
            } else {
              binaryValues[rowIndex] =
                  new Binary(
                      rawData
                          .get(columnIndex)
                          .get(rowIndex)
                          .toString()
                          .getBytes(StandardCharsets.UTF_8));
            }
          }
          columns[columnIndex] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Invalid input: " + dataTypes[columnIndex]);
      }
    }
    return columns;
  }
}
