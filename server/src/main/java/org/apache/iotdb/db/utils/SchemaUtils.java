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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaUtils {

  private SchemaUtils() {}

  private static final Map<TSDataType, Set<TSEncoding>> schemaChecker =
      new EnumMap<>(TSDataType.class);

  static {
    Set<TSEncoding> booleanSet = new HashSet<>();
    booleanSet.add(TSEncoding.PLAIN);
    booleanSet.add(TSEncoding.RLE);
    schemaChecker.put(TSDataType.BOOLEAN, booleanSet);

    Set<TSEncoding> intSet = new HashSet<>();
    intSet.add(TSEncoding.PLAIN);
    intSet.add(TSEncoding.RLE);
    intSet.add(TSEncoding.TS_2DIFF);
    intSet.add(TSEncoding.GORILLA);
    intSet.add(TSEncoding.ZIGZAG);
    intSet.add(TSEncoding.FREQ);
    intSet.add(TSEncoding.CHIMP);
    schemaChecker.put(TSDataType.INT32, intSet);
    schemaChecker.put(TSDataType.INT64, intSet);

    Set<TSEncoding> floatSet = new HashSet<>();
    floatSet.add(TSEncoding.PLAIN);
    floatSet.add(TSEncoding.RLE);
    floatSet.add(TSEncoding.TS_2DIFF);
    floatSet.add(TSEncoding.GORILLA_V1);
    floatSet.add(TSEncoding.GORILLA);
    floatSet.add(TSEncoding.FREQ);
    floatSet.add(TSEncoding.CHIMP);
    schemaChecker.put(TSDataType.FLOAT, floatSet);
    schemaChecker.put(TSDataType.DOUBLE, floatSet);

    Set<TSEncoding> textSet = new HashSet<>();
    textSet.add(TSEncoding.PLAIN);
    textSet.add(TSEncoding.DICTIONARY);
    schemaChecker.put(TSDataType.TEXT, textSet);
  }

  public static List<TSDataType> getSeriesTypesByPaths(Collection<? extends PartialPath> paths) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (PartialPath path : paths) {
      dataTypes.add(path == null ? null : path.getSeriesType());
    }
    return dataTypes;
  }

  /**
   * If the datatype of 'aggregation' depends on 'measurementDataType' (min_value, max_value),
   * return 'measurementDataType' directly, or return a list whose elements are all the datatype of
   * 'aggregation' and its length is the same as 'measurementDataType'.
   *
   * @param measurementDataType
   * @param aggregation
   * @return
   */
  public static List<TSDataType> getAggregatedDataTypes(
      List<TSDataType> measurementDataType, String aggregation) {
    TSDataType dataType = getAggregationType(aggregation);
    if (dataType != null) {
      return Collections.nCopies(measurementDataType.size(), dataType);
    }
    return measurementDataType;
  }

  public static TSDataType getSeriesTypeByPath(PartialPath path, String aggregation) {
    TSDataType dataType = getAggregationType(aggregation);
    if (dataType != null) {
      return dataType;
    } else {
      return path.getSeriesType();
    }
  }

  /**
   * @param aggregation aggregation function
   * @return the data type of the aggregation or null if it aggregation is null
   */
  public static TSDataType getAggregationType(String aggregation) {
    if (aggregation == null) {
      return null;
    }
    switch (aggregation.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.TIME_DURATION:
        return TSDataType.INT64;
      case SqlConstant.AVG:
      case SqlConstant.SUM:
        return TSDataType.DOUBLE;
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
      default:
        return null;
    }
  }

  /**
   * judge whether the order of aggregation calculation is consistent with the order of traversing
   * data
   */
  public static boolean isConsistentWithScanOrder(
      TAggregationType aggregationFunction, Ordering scanOrder) {
    boolean ascending = scanOrder == Ordering.ASC;
    switch (aggregationFunction) {
      case MIN_TIME:
      case FIRST_VALUE:
        return ascending;
      case MAX_TIME:
      case LAST_VALUE:
        return !ascending;
      case SUM:
      case MIN_VALUE:
      case MAX_VALUE:
      case EXTREME:
      case COUNT:
      case AVG:
      case TIME_DURATION:
        return true;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Aggregation function: %s", aggregationFunction));
    }
  }

  public static void checkDataTypeWithEncoding(TSDataType dataType, TSEncoding encoding)
      throws MetadataException {
    if (!schemaChecker.get(dataType).contains(encoding)) {
      throw new MetadataException(
          String.format("encoding %s does not support %s", encoding, dataType), true);
    }
  }

  public static List<TAggregationType> splitPartialAggregation(TAggregationType aggregationType) {
    switch (aggregationType) {
      case FIRST_VALUE:
        return Collections.singletonList(TAggregationType.MIN_TIME);
      case LAST_VALUE:
        return Collections.singletonList(TAggregationType.MAX_TIME);
      case AVG:
        return Arrays.asList(TAggregationType.COUNT, TAggregationType.SUM);
      case TIME_DURATION:
        return Arrays.asList(TAggregationType.MAX_TIME, TAggregationType.MIN_TIME);
      case SUM:
      case MIN_VALUE:
      case MAX_VALUE:
      case EXTREME:
      case COUNT:
      case MIN_TIME:
      case MAX_TIME:
      case COUNT_IF:
        return Collections.emptyList();
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Aggregation function: %s", aggregationType));
    }
  }
}
