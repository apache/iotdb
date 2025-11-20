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
package org.apache.iotdb.db.it.utils;

import org.apache.tsfile.enums.TSDataType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for TSDataType operations in integration tests. This class provides helper methods
 * to filter out unsupported data types that should not be used in tests.
 *
 * <p>Note: VECTOR and UNKNOWN are internal types that are not supported for general use. If new
 * unsupported types are added in the future (e.g., OBJECT), update the {@link
 * #getUnsupportedTypes()} method.
 *
 * <p>Usage in IT tests:
 *
 * <pre>{@code
 * Set<TSDataType> dataTypes = TSDataTypeTestUtils.getSupportedTypes();
 * for (TSDataType from : dataTypes) {
 *   for (TSDataType to : dataTypes) {
 *     // test logic
 *   }
 * }
 * }</pre>
 *
 * <p>To find this utility class quickly, search for: "TSDataTypeTestUtils" or "getSupportedTypes"
 */
public class TSDataTypeTestUtils {

  private TSDataTypeTestUtils() {
    // utility class
  }

  /**
   * Get the set of unsupported TSDataType values that should be filtered out in tests.
   *
   * <p>Currently includes: VECTOR, UNKNOWN
   *
   * <p>If new unsupported types are added (e.g., OBJECT), add them here.
   *
   * @return Set of unsupported TSDataType values
   */
  public static Set<TSDataType> getUnsupportedTypes() {
    Set<TSDataType> unsupported = new HashSet<>();
    unsupported.add(TSDataType.VECTOR);
    unsupported.add(TSDataType.UNKNOWN);
    // Add other unsupported types here if needed in the future
    // Example: if OBJECT type is not supported, uncomment the line below
    unsupported.add(TSDataType.OBJECT);
    return unsupported;
  }

  /**
   * Check if a TSDataType is supported for general use (not an internal type).
   *
   * @param dataType the TSDataType to check
   * @return true if the type is supported, false otherwise
   */
  public static boolean isSupportedType(TSDataType dataType) {
    return !getUnsupportedTypes().contains(dataType);
  }

  /**
   * Get all supported TSDataType values (filters out unsupported types).
   *
   * <p>This method filters out VECTOR, UNKNOWN, and any other types returned by {@link
   * #getUnsupportedTypes()}.
   *
   * @return Set of supported TSDataType values
   */
  public static Set<TSDataType> getSupportedTypes() {
    Set<TSDataType> allTypes = new HashSet<>(Arrays.asList(TSDataType.values()));
    allTypes.removeAll(getUnsupportedTypes());
    return allTypes;
  }

  /**
   * Get all supported TSDataType values as a List (filters out unsupported types).
   *
   * @return List of supported TSDataType values
   */
  public static List<TSDataType> getSupportedTypesList() {
    return Arrays.stream(TSDataType.values())
        .filter(TSDataTypeTestUtils::isSupportedType)
        .collect(Collectors.toList());
  }

  /**
   * Filter a collection of TSDataType values to only include supported types.
   *
   * @param dataTypes collection of TSDataType values to filter
   * @return Set containing only supported types
   */
  public static Set<TSDataType> filterSupportedTypes(Set<TSDataType> dataTypes) {
    Set<TSDataType> result = new HashSet<>(dataTypes);
    result.removeAll(getUnsupportedTypes());
    return result;
  }
}
