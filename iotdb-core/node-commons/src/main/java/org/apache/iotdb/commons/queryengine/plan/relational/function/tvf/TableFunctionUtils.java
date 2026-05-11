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

package org.apache.iotdb.commons.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableFunctionUtils {
  private static final String INVALID_OPTIONS_FORMAT = "Invalid options: %s";

  public static Map<String, String> parseOptions(String options) {
    if (options.isEmpty()) {
      return Collections.emptyMap();
    }
    String[] optionArray = options.split(",");
    if (optionArray.length == 0) {
      throw new SemanticException(String.format(INVALID_OPTIONS_FORMAT, options));
    }

    Map<String, String> optionsMap = new HashMap<>(optionArray.length);
    for (String option : optionArray) {
      int index = option.indexOf('=');
      if (index == -1 || index == option.length() - 1) {
        throw new SemanticException(String.format(INVALID_OPTIONS_FORMAT, option));
      }
      String key = option.substring(0, index).trim();
      String value = option.substring(index + 1).trim();
      optionsMap.put(key, value);
    }
    return optionsMap;
  }

  private static final Set<Type> ALLOWED_INPUT_TYPES = new HashSet<>();

  static {
    ALLOWED_INPUT_TYPES.add(Type.INT32);
    ALLOWED_INPUT_TYPES.add(Type.INT64);
    ALLOWED_INPUT_TYPES.add(Type.FLOAT);
    ALLOWED_INPUT_TYPES.add(Type.DOUBLE);
  }

  // only allow for INT32, INT64, FLOAT, DOUBLE
  public static void checkType(Type type, String columnName) {
    if (!ALLOWED_INPUT_TYPES.contains(type)) {
      throw new SemanticException(
          String.format(
              "The type of the column [%s] is [%s], only INT32, INT64, FLOAT, DOUBLE is allowed",
              columnName, type));
    }
  }
}
