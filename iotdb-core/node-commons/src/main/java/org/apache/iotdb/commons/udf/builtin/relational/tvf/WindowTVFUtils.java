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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.udf.api.exception.UDFColumnNotFoundException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.type.Type;

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

public class WindowTVFUtils {
  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  /**
   * Find the index of the column in the table argument.
   *
   * @param tableArgument the table argument
   * @param expectedFieldName the expected field name
   * @param expectedTypes the expected types
   * @return the index of the time column, -1 if not found
   */
  public static int findColumnIndex(
      TableArgument tableArgument, String expectedFieldName, Set<Type> expectedTypes)
      throws UDFException {
    for (int i = 0; i < tableArgument.getFieldTypes().size(); i++) {
      Optional<String> fieldName = tableArgument.getFieldNames().get(i);
      if (fieldName.isPresent() && expectedFieldName.equalsIgnoreCase(fieldName.get())) {
        if (!expectedTypes.contains(tableArgument.getFieldTypes().get(i))) {
          throw new UDFTypeMismatchException(
              String.format(
                  CommonMessages.EXCEPTION_TYPE_COLUMN_ARG_NOT_AS_EXPECTED_7A81636E,
                  expectedFieldName));
        }
        return i;
      }
    }
    throw new UDFColumnNotFoundException(
        String.format(
            CommonMessages.EXCEPTION_REQUIRED_COLUMN_ARG_NOT_FOUND_SOURCE_TABLE_ARGUMENT_993E1C08,
            expectedFieldName));
  }

  static BigInteger getWindowStart(long source, long origin, long interval) {
    return getWindowStart(source, origin, interval, BigInteger.ZERO);
  }

  static BigInteger getWindowStart(
      long source, long origin, long interval, BigInteger distanceOffset) {
    BigInteger intervalValue = BigInteger.valueOf(interval);
    BigInteger stepCount =
        BigInteger.valueOf(source)
            .subtract(BigInteger.valueOf(origin))
            .add(distanceOffset)
            .divide(intervalValue);
    return BigInteger.valueOf(origin).add(stepCount.multiply(intervalValue));
  }

  static long saturateToLong(BigInteger value) {
    if (value.compareTo(BIG_LONG_MAX) > 0) {
      return Long.MAX_VALUE;
    }
    if (value.compareTo(BIG_LONG_MIN) < 0) {
      return Long.MIN_VALUE;
    }
    return value.longValue();
  }
}
