/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.util.visitor;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class SubtractionResolver {
  private static final Map<Type, Map<Type, Type>> conditionMap = new HashMap<>();

  static {
    addCondition(INT32, INT32, INT32);
    addCondition(INT32, INT64, INT64);
    addCondition(INT32, FLOAT, FLOAT);
    addCondition(INT32, DOUBLE, DOUBLE);

    addCondition(INT64, INT32, INT64);
    addCondition(INT64, INT64, INT64);
    addCondition(INT64, FLOAT, FLOAT);
    addCondition(INT64, DOUBLE, DOUBLE);

    addCondition(FLOAT, INT32, FLOAT);
    addCondition(FLOAT, INT64, FLOAT);
    addCondition(FLOAT, FLOAT, FLOAT);
    addCondition(FLOAT, DOUBLE, DOUBLE);

    addCondition(DOUBLE, INT32, DOUBLE);
    addCondition(DOUBLE, INT64, DOUBLE);
    addCondition(DOUBLE, FLOAT, DOUBLE);
    addCondition(DOUBLE, DOUBLE, DOUBLE);

    addCondition(DATE, INT32, DATE);
    addCondition(DATE, INT64, DATE);

    addCondition(TIMESTAMP, INT32, TIMESTAMP);
    addCondition(TIMESTAMP, INT64, TIMESTAMP);
  }

  public static void addCondition(Type condition1, Type condition2, Type result) {
    conditionMap.computeIfAbsent(condition1, k -> new HashMap<>()).put(condition2, result);
  }

  public static Type checkConditions(Type condition1, Type condition2) {
    Type result =
        conditionMap.getOrDefault(condition1, new HashMap<>()).getOrDefault(condition2, null);
    if (result == null) {
      throw new UnsupportedOperationException("Unsupported : " + condition1 + " - " + condition2);
    }
    return result;
  }
}
