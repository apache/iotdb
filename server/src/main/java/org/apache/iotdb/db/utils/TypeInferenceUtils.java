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

import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TypeInferenceUtils {

  private TypeInferenceUtils() {

  }

  static boolean isNumber(String s) {
    try {
      Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  private static boolean isBoolean(String s) {
    return s.equalsIgnoreCase(SQLConstant.BOOLEN_TRUE) || s
        .equalsIgnoreCase(SQLConstant.BOOLEN_FALSE);
  }

  /**
   * Get predicted DataType of the given value
   */
  public static TSDataType getPredictedDataType(Object value) {
    if (value instanceof Boolean || (value instanceof String && isBoolean((String) value))) {
      return TSDataType.BOOLEAN;
    } else if (value instanceof Number || (value instanceof String && isNumber((String) value))) {
      String v = String.valueOf(value);
      if (!v.contains(".")) {
        return TSDataType.INT64;
      } else {
        return TSDataType.DOUBLE;
      }
    } else {
      return TSDataType.TEXT;
    }
  }
}
